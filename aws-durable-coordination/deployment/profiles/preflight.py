#!/usr/bin/env python3
"""Offline contract checks. Reads owner observations; never probes or changes AWS."""
import argparse
import json
import re
from pathlib import Path
import yaml

_BASE = Path(__file__).resolve().parents[1]
TEMPLATE = (_BASE / 'aws-durable-queue-async.yaml' if (_BASE / 'aws-durable-queue-async.yaml').is_file()
            else _BASE / 'src/main/resources/aws-durable-queue-async.yaml')

class Loader(yaml.SafeLoader):
    pass

def tagged(loader, tag, node):
    value = (loader.construct_scalar(node) if isinstance(node, yaml.ScalarNode) else
             loader.construct_sequence(node) if isinstance(node, yaml.SequenceNode) else loader.construct_mapping(node))
    return {('Ref' if tag == 'Ref' else 'Condition' if tag == 'Condition' else 'Fn::' + tag): value}
Loader.add_multi_constructor('!', tagged)


def load_template(path=TEMPLATE):
    return yaml.load(Path(path).read_text(), Loader=Loader)


def resolve(value, parameters, conditions=None):
    conditions = conditions or {}
    if isinstance(value, list):
        return [resolved for item in value if (resolved := resolve(item, parameters, conditions)) is not None]
    if not isinstance(value, dict):
        return value
    if set(value) == {'Ref'}:
        return None if value['Ref'] == 'AWS::NoValue' else parameters.get(value['Ref'], value)
    if set(value) == {'Condition'}:
        return conditions[value['Condition']]
    for op in ['If', 'Equals', 'And', 'Or', 'Not', 'Contains', 'EachMemberEquals', 'Join']:
        if set(value) != {'Fn::' + op}:
            continue
        args = value['Fn::' + op]
        if op == 'If':
            return resolve(args[1 if conditions[args[0]] else 2], parameters, conditions)
        args = [resolve(arg, parameters, conditions) for arg in args]
        if op == 'Equals': return args[0] == args[1]
        if op == 'And': return all(args)
        if op == 'Or': return any(args)
        if op == 'Not': return not args[0]
        if op == 'Contains': return args[1] in args[0]
        if op == 'EachMemberEquals': return all(item == args[1] for item in args[0])
        if op == 'Join': return args[0].join(args[1])
    return {key: resolved for key, item in value.items()
            if (resolved := resolve(item, parameters, conditions)) is not None}



def rule_value(value, parameters, selected):
    """Evaluate only Rules/Conditions intrinsics; never treat unresolved expressions as truthy."""
    if isinstance(value, list):
        return [rule_value(item, parameters, selected) for item in value]
    if not isinstance(value, dict):
        return value
    if len(value) != 1:
        raise ValueError('Malformed rule expression')
    operator, arguments = next(iter(value.items()))
    if operator == 'Ref':
        if not isinstance(arguments, str) or arguments not in parameters:
            raise ValueError('Unresolved rule parameter reference')
        return parameters[arguments]
    if operator == 'Condition':
        if not isinstance(arguments, str): raise ValueError('Invalid condition reference')
        return selected[arguments] # Conditions are ordered by dependency, not source position.
    arities = {'Fn::Equals': (2, 2), 'Fn::And': (2, 10), 'Fn::Or': (2, 10),
               'Fn::Not': (1, 1), 'Fn::Contains': (2, 2), 'Fn::EachMemberEquals': (2, 2)}
    if operator not in arities:
        raise ValueError('Unsupported rule intrinsic: ' + operator)
    minimum, maximum = arities[operator]
    if not isinstance(arguments, list) or not minimum <= len(arguments) <= maximum:
        raise ValueError('Invalid rule intrinsic arguments: ' + operator)
    args = [rule_value(arg, parameters, selected) for arg in arguments]
    if operator == 'Fn::Equals':
        if any(type(arg) not in (str, int, float, bool) for arg in args):
            raise ValueError('Rule equality requires resolved scalar operands')
        return args[0] == args[1]
    if operator in ('Fn::And', 'Fn::Or', 'Fn::Not'):
        if any(type(arg) is not bool for arg in args):
            raise ValueError('Rule boolean operator requires boolean operands')
        if operator == 'Fn::And': return all(args)
        if operator == 'Fn::Or': return any(args)
        return not args[0]
    if not isinstance(args[0], list) or any(type(item) is not str for item in args[0]) or type(args[1]) is not str:
        raise ValueError('Rule membership requires a resolved string list and string value')
    if operator == 'Fn::Contains': return args[1] in args[0]
    return all(item == args[1] for item in args[0])


def rule_boolean(value, parameters, selected):
    try: result = rule_value(value, parameters, selected)
    except KeyError as error: raise ValueError('Unresolved rule condition reference') from error
    if type(result) is not bool: raise ValueError('Rule condition/assertion must resolve to boolean')
    return result


def defaults(template):
    return {key: definition['Default'].split(',') if definition['Type'] == 'CommaDelimitedList' else definition['Default']
            for key, definition in template['Parameters'].items() if 'Default' in definition}


def conditions(template, parameters):
    result = {}
    pending = dict(template.get('Conditions', {}))
    while pending:
        progressed = False
        for key, value in list(pending.items()):
            try:
                resolved = rule_value(value, parameters, result)
                if type(resolved) is not bool: raise ValueError('Deployment condition must resolve to boolean')
                result[key] = resolved
            except KeyError: continue
            del pending[key]
            progressed = True
        if not progressed: raise ValueError('Unresolved deployment condition')
    return result


def check(template, supplied, inventory=None, guard=None, account_id=None):
    if set(supplied) - set(template['Parameters']): raise ValueError('Unknown deployment parameter')
    parameters = defaults(template) | supplied
    for name, spec in template['Parameters'].items():
        if name not in parameters: raise ValueError('Missing deployment parameter: ' + name)
        value = parameters[name]
        if spec['Type'] == 'CommaDelimitedList':
            value = value.split(',') if isinstance(value, str) else value
            parameters[name] = value
            values = value
        else: values = [value]
        for item in values:
            if 'AllowedValues' in spec and item not in spec['AllowedValues']: raise ValueError('Invalid parameter: ' + name)
            if 'AllowedPattern' in spec and not re.fullmatch(spec['AllowedPattern'], str(item)): raise ValueError('Invalid parameter: ' + name)
            if 'MinValue' in spec and float(item) < spec['MinValue']: raise ValueError('Invalid parameter: ' + name)
            if 'MaxValue' in spec and float(item) > spec['MaxValue']: raise ValueError('Invalid parameter: ' + name)
    # Optional observed deployment context; never infer the role account from a KMS
    # key, which may have a separately owned/cross-account authority.
    if account_id is not None:
        if not isinstance(account_id, str) or not re.fullmatch(r'[0-9]{12}', account_id):
            raise ValueError('Invalid owner deployment account')
        boundary = parameters.get('RuntimePermissionsBoundaryArn', '')
        if boundary and boundary.split(':')[4] != account_id:
            raise ValueError('Runtime permissions boundary must belong to the deployment account')
    selected = conditions(template, parameters)
    for name, rule in template['Rules'].items():
        if 'RuleCondition' in rule and not rule_boolean(rule['RuleCondition'], parameters, selected): continue
        for assertion in rule['Assertions']:
            if not rule_boolean(assertion['Assert'], parameters, selected): raise ValueError(name + ': ' + assertion['AssertDescription'])
    if parameters['DeploymentProfile'] == 'RESTRICTED_PILOT':
        if not inventory or not guard: raise ValueError('Owner signing inventory and retention guard required')
        retained = parameters['SigningParameterAccessArns']
        if len(retained) != len(set(retained)) or '' in retained: raise ValueError('Invalid retained parameter set')
        if set(inventory) != set(retained): raise ValueError('Inventory must cover every retained parameter exactly')
        for arn, observed in inventory.items():
            if observed != {'arn': arn, 'version': 1, 'type': 'SecureString', 'tier': 'Standard', 'keyKind': 'AWS_MANAGED_SSM'}:
                raise ValueError('Signing parameter owner observation mismatch')
        # This is an owner receipt, not a substitute for IAM evaluation or retained-work inventory.
        if guard != {'schemaVersion': 1, 'parameterArns': sorted(retained), 'denyOverwrite': True,
                     'denyDelete': True, 'denyHistoryAndPathReads': True, 'retainUntilDrained': True,
                     'iamOnlyAccountAuthorityAccepted': True}:
            raise ValueError('Incomplete immutable signing retention guard receipt')
    return parameters


def resources(template, parameters):
    selected = conditions(template, parameters)
    return {name: resolve(resource, parameters, selected)
            for name, resource in template['Resources'].items()
            if 'Condition' not in resource or selected[resource['Condition']]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--parameters', required=True, type=Path)
    parser.add_argument('--signing-inventory', type=Path)
    parser.add_argument('--account-id', help='Owner-observed 12-digit deployment account; checks optional boundary ownership, not live IAM')
    parser.add_argument('--retention-guard', type=Path)
    args = parser.parse_args()
    try:
        check(load_template(), json.loads(args.parameters.read_text()),
              json.loads(args.signing_inventory.read_text()) if args.signing_inventory else None,
              json.loads(args.retention_guard.read_text()) if args.retention_guard else None,
              account_id=args.account_id)
    except (ValueError, TypeError, KeyError) as error:
        parser.exit(1, 'Preflight rejected: ' + str(error) + '\n')
    print('Offline deployment contract passed; owner observations are not live AWS verification or deployment approval.')

if __name__ == '__main__': main()
