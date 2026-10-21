import copy
import hashlib
import json
import os
from pathlib import Path
import unittest
import preflight as p

TEMPLATE = Path(os.environ.get('TPF_TEST_TEMPLATE', p.TEMPLATE))


def fixture(profile='RESTRICTED_PILOT'):
    template = p.load_template(TEMPLATE)
    values = p.defaults(template)
    for name, spec in template['Parameters'].items():
        if name in values: continue
        if 'AllowedValues' in spec: values[name] = spec['AllowedValues'][0]
        elif spec['Type'] == 'Number': values[name] = spec.get('MinValue', 1)
        elif name.endswith('CodeSha256'): values[name] = 'a' * 43 + '='
        elif name.endswith('Digest'): values[name] = 'a' * 64
        elif name.endswith('Arn'): values[name] = 'arn:aws:kms:us-east-2:123456789012:key/1234-abcd'
        elif spec['Type'] == 'CommaDelimitedList': values[name] = ['arn:aws:dynamodb:us-east-2:123456789012:table/semantic']
        else: values[name] = 'test-value'
    values['AlarmTopicArn'] = 'arn:aws:sns:us-east-2:123456789012:test'
    values['DeploymentProfile'] = profile
    values['TransitionWorkerPlacement'] = 'FUNCTION'
    values['SemanticKmsKeyArns'] = ['']
    values['SigningSecretAccessArns'] = ['']
    values['SigningKmsAccessArns'] = ['']
    if profile == 'RESTRICTED_PILOT':
        values['AlarmTopicArn']=''
        values.update(SigningSource='SSM_STANDARD', RestrictedPilotValidation='validated',
                      SigningParameterLifecycleValidation='validated')
        for purpose, label in [('ResumeToken', 'resume-token'), ('WorkerShared', 'worker')]:
            values[purpose + 'ParameterArn'] = 'arn:aws:ssm:us-east-2:123456789012:parameter/tpf/durable/pilot/' + label + '/g-' + 'a' * 32
            values[purpose + 'ParameterVersion'] = '1'
        values['SigningParameterAccessArns'] = [values['ResumeTokenParameterArn'], values['WorkerSharedParameterArn']]
    else:
        key = values['HostKmsKeyArn']
        values['SemanticKmsKeyArns'] = [key]
        values['SigningKmsKeyArn'] = key
        values['SigningKmsAccessArns'] = [key]
        for purpose in ['ResumeToken', 'WorkerShared']:
            values[purpose + 'SecretArn'] = 'arn:aws:secretsmanager:us-east-2:123456789012:secret:' + purpose + '-abcdef'
            values[purpose + 'SecretVersion'] = 'a' * 32
        values['SigningSecretAccessArns'] = [values['ResumeTokenSecretArn'], values['WorkerSharedSecretArn']]
    inventory = {arn: {'arn': arn, 'version': 1, 'type': 'SecureString', 'tier': 'Standard', 'keyKind': 'AWS_MANAGED_SSM'}
                 for arn in values.get('SigningParameterAccessArns', []) if arn}
    guard = {'schemaVersion': 1, 'parameterArns': sorted(inventory), 'denyOverwrite': True, 'denyDelete': True,
             'denyHistoryAndPathReads': True, 'retainUntilDrained': True, 'iamOnlyAccountAuthorityAccepted': True}
    return values, inventory, guard


def graph(template, profile):
    return p.resources(template, {'DeploymentProfile': profile,
                       'SigningSource': 'SECRETS_MANAGER' if profile == 'HARDENED' else 'SSM_STANDARD',
                       'TransitionWorkerPlacement': 'FUNCTION', 'RuntimePermissionsBoundaryArn': ''})


def legacy_code_location_contract(template):
    """Compare all existing fields while separately proving the exact CodeUri rewrite."""
    template = copy.deepcopy(template)
    count = 0
    for resource in template['Resources'].values():
        if resource['Type'] == 'AWS::Serverless::Function':
            assert resource['Properties']['CodeUri'] == {'Fn::Sub': '${ActionArtifactUri}'}
            resource['Properties']['CodeUri'] = {'Ref': 'ActionArtifactUri'}
            count += 1
    assert count == 8
    return template


def normalized_hardened(template):
    resources = graph(legacy_code_location_contract(template), 'HARDENED')
    for resource in resources.values():
        resource.pop('Condition', None)
        env = resource.get('Properties', {}).get('Environment', {}).get('Variables', {})
        env.pop('TPF_AWS_DURABLE_SIGNING_SOURCE', None) # Existing reader's explicit default; no changed key bytes.
    return resources


class Profiles(unittest.TestCase):
    def setUp(self): self.template = p.load_template(TEMPLATE)

    def test_default_hardened_resource_graph_matches_published_runtime63(self):
        self.assertEqual('HARDENED', self.template['Parameters']['DeploymentProfile']['Default'])
        digest = hashlib.sha256(json.dumps(normalized_hardened(self.template), sort_keys=True).encode()).hexdigest()
        self.assertEqual('af88bff8b42a5696793faee2356ae8610f031a0d1159afcabeb632d6b80ba222', digest)

    def test_optional_boundary_default_preserves_all_existing_parameters(self):
        # The old published 73-parameter input remains valid; only this optional
        # field is new and no release-manifest schema field is changed.
        self.assertEqual(74, len(self.template['Parameters']))
        spec = self.template['Parameters']['RuntimePermissionsBoundaryArn']
        self.assertEqual('', spec['Default'])
        for profile in ['HARDENED', 'RESTRICTED_PILOT']:
            values, inventory, guard = fixture(profile)
            values.pop('RuntimePermissionsBoundaryArn')
            checked = p.check(self.template, values, inventory, guard)
            self.assertEqual('', checked['RuntimePermissionsBoundaryArn'])
            roles = {name: resource for name, resource in p.resources(self.template, checked).items()
                     if resource['Type'] == 'AWS::IAM::Role'}
            self.assertEqual({'DueWorkRole', 'DurableRole', 'IngressRole', 'ActionRole', 'WorkerRole', 'WakeupRole'}, set(roles))
            self.assertTrue(all('PermissionsBoundary' not in role['Properties'] for role in roles.values()))

    def test_selected_boundary_changes_only_all_six_role_properties(self):
        boundary = 'arn:aws:iam::123456789012:policy/tpf/runtime-boundary'
        for profile in ['HARDENED', 'RESTRICTED_PILOT']:
            values, inventory, guard = fixture(profile)
            baseline = p.resources(self.template, p.check(self.template, values, inventory, guard))
            values['RuntimePermissionsBoundaryArn'] = boundary
            checked = p.check(self.template, values, inventory, guard, account_id='123456789012')
            selected = p.resources(self.template, checked)
            self.assertEqual(set(baseline), set(selected))
            count = 0
            for name, resource in selected.items():
                if resource['Type'] == 'AWS::IAM::Role':
                    count += 1
                    self.assertEqual(boundary, resource['Properties'].pop('PermissionsBoundary'))
                self.assertEqual(baseline[name], resource, name)
            self.assertEqual(6, count)

    def test_boundary_parameter_rejects_malformed_wildcard_or_other_authorities(self):
        for invalid in ['*', 'arn:aws:iam::123456789012:role/runtime',
                        'arn:aws:iam::aws:policy/AdministratorAccess',
                        'arn:aws:iam::123456789012:policy/*',
                        'arn:aws:iam::123456789012:policy/',
                        'arn:aws:iam:us-east-2:123456789012:policy/runtime',
                        'arn:aws:iam::123456789012:policy/runtime?other',
                        'arn:aws:iam::123456789012:policy/runtime\n']:
            with self.subTest(boundary=invalid):
                values, inventory, guard = fixture()
                values['RuntimePermissionsBoundaryArn'] = invalid
                with self.assertRaisesRegex(ValueError, 'Invalid parameter: RuntimePermissionsBoundaryArn'):
                    p.check(self.template, values, inventory, guard)

    def test_observed_account_rejects_foreign_boundary_without_claiming_live_iam(self):
        values, inventory, guard = fixture()
        values['RuntimePermissionsBoundaryArn'] = 'arn:aws:iam::999999999999:policy/foreign'
        with self.assertRaisesRegex(ValueError, 'deployment account'):
            p.check(self.template, values, inventory, guard, account_id='123456789012')
        with self.assertRaisesRegex(ValueError, 'Invalid owner deployment account'):
            p.check(self.template, values, inventory, guard, account_id='not-an-account')
        # Without actual owner context, syntax success is not ownership evidence.
        p.check(self.template, values, inventory, guard)

    def test_both_profiles_preflight(self):
        for profile in ['HARDENED', 'RESTRICTED_PILOT']:
            args = fixture(profile)
            p.check(self.template, *args)

    def test_restricted_recovery_graph_is_identical(self):
        hardened, pilot = (graph(self.template, name) for name in ['HARDENED', 'RESTRICTED_PILOT'])
        self.assertEqual(64, len(set(hardened) - set(pilot)))
        self.assertTrue(all(hardened[name]['Type'] == 'AWS::CloudWatch::Alarm' for name in set(hardened) - set(pilot)))
        for name in pilot:
            self.assertEqual(hardened[name]['Type'], pilot[name]['Type'])
            if pilot[name]['Type'] in ['AWS::Lambda::EventSourceMapping', 'AWS::Events::Rule', 'AWS::Lambda::Version',
                                      'AWS::Lambda::Alias', 'AWS::Lambda::EventInvokeConfig', 'AWS::Lambda::Permission', 'AWS::SQS::QueuePolicy']:
                self.assertEqual(hardened[name], pilot[name], name)
            for retention in ['DeletionPolicy', 'UpdateReplacePolicy']:
                self.assertEqual(hardened[name].get(retention), pilot[name].get(retention), name)
        # Every runtime principal/trust and non-encryption native action stays identical.
        for name,resource in pilot.items():
            if resource['Type']=='AWS::IAM::Role':
                a,b=(copy.deepcopy(r[name]) for r in [hardened,pilot])
                for r in [a,b]:
                    for policy in r['Properties'].get('Policies',[]):
                        policy['PolicyDocument']['Statement']=[st for st in policy['PolicyDocument']['Statement']
                            if not any(action.startswith(('kms:','ssm:','secretsmanager:'))
                                for action in ([st['Action']] if isinstance(st['Action'],str) else st['Action']))]
                self.assertEqual(a,b,name)
            if resource['Type'] in ['AWS::Lambda::Function','AWS::Serverless::Function']:
                a,b=(copy.deepcopy(r[name]) for r in [hardened,pilot])
                for r in [a,b]:
                    props=r['Properties'];props.pop('KmsKeyArn',None)
                    props.get('DurableConfig',{}).pop('KMSKeyArn',None)
                    env=props.get('Environment',{}).get('Variables',{})
                    for key in list(env):
                        if key=='TPF_AWS_DURABLE_SIGNING_SOURCE' or any(key.endswith(suffix) for suffix in
                            ['RESUME_TOKEN_SECRET_ARN','RESUME_TOKEN_SECRET_VERSION','WORKER_SECRET_ARN','WORKER_SECRET_VERSION',
                             'RESUME_TOKEN_PARAMETER_ARN','RESUME_TOKEN_PARAMETER_VERSION','WORKER_PARAMETER_ARN','WORKER_PARAMETER_VERSION']):
                            del env[key]
                self.assertEqual(a,b,name)
        mappings = [resource for resource in pilot.values() if resource['Type'] == 'AWS::Lambda::EventSourceMapping']
        self.assertEqual(7, len(mappings))
        self.assertEqual(hardened['DueWorkRule'], pilot['DueWorkRule'])
        for name in [name for name, r in pilot.items() if r['Type'] == 'AWS::SQS::Queue']:
            a, b = copy.deepcopy(hardened[name]), copy.deepcopy(pilot[name])
            for r in [a,b]:
                for key in ['KmsMasterKeyId','SqsManagedSseEnabled']: r['Properties'].pop(key,None)
            self.assertEqual(a,b,name)
        a,b=(copy.deepcopy(r['CallbackBindingTable']) for r in [hardened,pilot])
        for r in [a,b]:r['Properties'].pop('SSESpecification')
        self.assertEqual(a,b)

    def test_encryption_and_runtime_iam_are_consistent(self):
        pilot=graph(self.template,'RESTRICTED_PILOT')
        for name,r in pilot.items():
            props=r.get('Properties',{})
            if r['Type']=='AWS::SQS::Queue':
                if name in ['DueWorkQueue','DueWorkDlq','RepairFailureDlq']:
                    self.assertEqual({'Ref':'HostKmsKeyArn'},props['KmsMasterKeyId'])
                else:
                    self.assertTrue(props['SqsManagedSseEnabled']);self.assertNotIn('KmsMasterKeyId',props)
            if r['Type']=='AWS::Logs::LogGroup':self.assertNotIn('KmsKeyId',props)
            if r['Type']=='AWS::IAM::Role':
                statements=props.get('Policies',[{}])[0].get('PolicyDocument',{}).get('Statement',[])
                for st in statements:
                    text=json.dumps(st)
                    self.assertNotIn('SigningKmsAccessArns',text);self.assertNotIn('SemanticKmsKeyArns',text)
                    self.assertNotIn('secretsmanager:GetSecretValue',text)
                    if 'HostKmsKeyArn' in text:
                        self.assertIn(name,['DueWorkRole','WakeupRole'])
                        self.assertIn('kms:EncryptionContext:aws:sqs:arn',text)
                    if 'ssm:' in text:
                        self.assertEqual('ssm:GetParameter',st['Action'])
                        self.assertEqual({'Ref':'SigningParameterAccessArns'},st['Resource'])
        self.assertFalse(pilot['CallbackBindingTable']['Properties']['SSESpecification']['SSEEnabled'])
        self.assertNotIn('KmsKeyArn',pilot['DurableFunction']['Properties'])
        self.assertNotIn('KMSKeyArn',pilot['DurableFunction']['Properties']['DurableConfig'])
        for name in ['ActionFunction','DueWorkFunction','AwaitWakeupFunction','WorkFunction']:
            env=pilot[name]['Properties']['Environment']['Variables']
            self.assertEqual('SSM_STANDARD',env['TPF_AWS_DURABLE_SIGNING_SOURCE'])
            self.assertNotIn('TPF_AWS_DURABLE_WORKER_SECRET_ARN',env)
            self.assertEqual({'Ref':'WorkerSharedParameterArn'},env['TPF_AWS_DURABLE_WORKER_PARAMETER_ARN'])

    def test_manifest_renders_both_discriminants_and_actual_log_references(self):
        import re
        for profile,placement in [('HARDENED','FUNCTION'),('HARDENED','COMPUTE'),('RESTRICTED_PILOT','FUNCTION')]:
            parameters,_,_=fixture(profile)
            parameters['TransitionWorkerPlacement']=placement
            selected=p.conditions(self.template,parameters)
            value=p.resolve(self.template['Outputs']['DeploymentReleaseManifest']['Value'],parameters,selected)
            def sub(node):
                if isinstance(node,dict) and set(node)=={'Ref'}:
                    return parameters.get(node['Ref'],'resolved-'+node['Ref'])
                if isinstance(node,dict) and 'Fn::Sub' in node:
                    spec=node['Fn::Sub']
                    text,variables=(spec,{}) if isinstance(spec,str) else spec
                    variables={key:sub(item) for key,item in variables.items()}
                    return re.sub(r'\$\{([^}]+)\}',lambda m:str(variables.get(m[1],parameters.get(m[1],'resolved-'+m[1]))),text)
                return node
            manifest=json.loads(sub(value))
            self.assertEqual(2,manifest['schemaVersion'])
            self.assertEqual(profile,manifest['deploymentProfile'])
            self.assertEqual('EVENTBRIDGE_RULE',manifest['scheduleProvider'])
            self.assertEqual(10,len(manifest['logGroupNames']))
            self.assertEqual('resolved-DurableFunctionLogs',manifest['logGroupNames']['durable'])
            expected_transition='' if placement=='COMPUTE' else 'resolved-TransitionWorkerFunctionLogs'
            self.assertEqual(expected_transition,manifest['logGroupNames']['transition'])
            if placement=='COMPUTE':
                self.assertEqual('',manifest['functionVersionArns']['transition'])
                self.assertEqual('',manifest['mappingIds']['transition'])
            signing=manifest['signing']
            if profile=='RESTRICTED_PILOT':
                self.assertEqual('SSM_STANDARD',signing['source'])
                self.assertEqual(1,signing['resumeToken']['version'])
                self.assertEqual(parameters['SigningParameterAccessArns'],signing['retainedParameterAccessArns'])
                self.assertEqual('SERVICE_OWNED_EXCEPT_EVENTBRIDGE_CMK',manifest['encryption']['host'])
                self.assertNotIn('versionId',signing['worker'])
                self.assertEqual([],manifest['semanticKmsKeyArns'])
            else:
                self.assertEqual('SECRETS_MANAGER',signing['source'])
                self.assertEqual(parameters['WorkerSharedSecretVersion'],signing['worker']['versionId'])

    def test_compute_manifest_only_references_available_resources(self):
        import re
        parameters,_,_=fixture('HARDENED')
        parameters['TransitionWorkerPlacement']='COMPUTE'
        selected=p.conditions(self.template,parameters)
        available=p.resources(self.template,parameters)
        self.assertNotIn('TransitionWorkerFunctionLogs',available)
        pseudo={'AWS::AccountId','AWS::NotificationARNs','AWS::NoValue','AWS::Partition','AWS::Region','AWS::StackId','AWS::StackName','AWS::URLSuffix'}
        def reference(name,variables=frozenset()):
            self.assertTrue(name in variables or name in parameters or name in pseudo or name.split('.')[0] in available,name)
        def walk(node):
            if isinstance(node,list):
                for item in node:walk(item)
            elif isinstance(node,dict):
                if set(node)=={'Ref'}:reference(node['Ref'])
                elif set(node)=={'Fn::GetAtt'}:
                    value=node['Fn::GetAtt'];reference(value if isinstance(value,str) else value[0])
                elif set(node)=={'Fn::Sub'}:
                    spec=node['Fn::Sub'];text,variables=(spec,{}) if isinstance(spec,str) else spec
                    for name in re.findall(r'\$\{([^}]+)\}',text):reference(name,variables)
                    walk(variables)
                else:
                    for value in node.values():walk(value)
        walk(p.resolve(self.template['Outputs']['DeploymentReleaseManifest']['Value'],parameters,selected))

    def test_rules_reject_unknown_or_unresolved_expressions_and_non_boolean_results(self):
        values,inventory,guard=fixture()
        invalid=[{'Fn::Unknown':[True]}, {'Ref':'MissingParameter'}, {'Condition':'MissingCondition'},
                 {'Fn::Equals':[{'Ref':'MissingParameter'},'accepted']},
                 {'Fn::And':[True,'truthy']}, {'Fn::Or':[False,1]}, {'Fn::Not':['false']},
                 {'Fn::Contains':['not-a-list','value']}, {'Fn::EachMemberEquals':[[1],'1']},
                 'truthy',1,[],{'unexpected':'truthy'}]
        for expression in invalid:
            for position in ['RuleCondition','Assert']:
                template=copy.deepcopy(self.template)
                rule={'Assertions':[{'Assert':True,'AssertDescription':'must fail closed'}]}
                if position=='Assert':rule['Assertions'][0]['Assert']=expression
                else:rule['RuleCondition']=expression
                template['Rules']['StrictEvaluationRegression']=rule
                with self.subTest(expression=expression,position=position),self.assertRaises(ValueError):
                    p.check(template,values,inventory,guard)
        for expression in [{'Fn::Unknown':[True]},{'Ref':'MissingParameter'},'truthy']:
            template=copy.deepcopy(self.template);template['Conditions']['InvalidCondition']=expression
            with self.subTest(condition=expression),self.assertRaises(ValueError):
                p.check(template,values,inventory,guard)

    def test_invalid_selections_and_mutable_versions_fail_closed(self):
        values,inventory,guard=fixture()
        mutations={'DeploymentProfile':'UNKNOWN','SigningSource':'SECRETS_MANAGER', 'RestrictedPilotValidation':'not-applicable',
                   'SigningParameterLifecycleValidation':'not-applicable','ReleaseIsolationMode':'DRAINED_RELEASE_MIGRATION',
                   'TransitionWorkerPlacement':'COMPUTE','ResumeTokenParameterVersion':'2',
                   'WorkerSharedParameterVersion':'latest','ResumeTokenParameterArn':values['WorkerSharedParameterArn'],
                   'WorkerSharedSecretArn':'arn:aws:secretsmanager:us-east-2:123456789012:secret:other-abcdef',
                   'SigningParameterAccessArns':[], 'AlarmTopicArn':'arn:aws:sns:us-east-2:123456789012:unused'}
        for key,value in mutations.items():
            with self.subTest(key=key),self.assertRaises(ValueError):p.check(self.template,values|{key:value},inventory,guard)
        for key in ['denyOverwrite','denyDelete','denyHistoryAndPathReads','retainUntilDrained','iamOnlyAccountAuthorityAccepted']:
            with self.subTest(key=key),self.assertRaises(ValueError):p.check(self.template,values,inventory,guard|{key:False})
        for key,value in [('tier','Advanced'),('type','String'),('version',2),('keyKind','CUSTOMER_MANAGED')]:
            invalid=copy.deepcopy(inventory);invalid[values['ResumeTokenParameterArn']][key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):p.check(self.template,values,invalid,guard)

if __name__=='__main__':unittest.main()
