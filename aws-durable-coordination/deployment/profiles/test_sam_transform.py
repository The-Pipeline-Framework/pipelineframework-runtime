"""Exercise both official local transform stages, without AWS or credentials.

The local LanguageExtensions processor is not the AWS-hosted macro. A deployment
owner must still inspect an AWS change-set preview before executing a new asset.
"""
from contextlib import contextmanager
import copy
import os
import itertools
import socket
import unittest
from unittest.mock import patch

from samcli.lib.cfn_language_extensions.sam_integration import expand_language_extensions
from samtranslator.model.exceptions import InvalidDocumentException
from samtranslator.translator.transform import transform

import preflight as p
from test_profiles import TEMPLATE, fixture

SAM_FUNCTIONS = {'ActionFunction', 'WorkFunction', 'AwaitCompletionFunction',
                 'TransitionWorkerFunction', 'AwaitWakeupFunction', 'ExecutionWakeupFunction',
                 'DueWorkFunction', 'ReconciliationFunction'}
TRANSFORMS = ['AWS::LanguageExtensions', 'AWS::Serverless-2016-10-31']
BUCKET = 'tpf-code-location-test'
KEY = 'releases/20261021/sha256/nested/action-lambda-svc.zip'
URI = 's3://' + BUCKET + '/' + KEY


class NoManagedPolicies:
    def load(self):
        raise AssertionError('Every existing function has an explicit role; no managed-policy lookup is required')


@contextmanager
def offline_processors():
    # Both official stages run inside the same credential and transport boundary.
    with patch.dict('os.environ', {'AWS_DEFAULT_REGION': 'us-east-2',
                                  'AWS_EC2_METADATA_DISABLED': 'true',
                                  'AWS_SHARED_CREDENTIALS_FILE': '/dev/null',
                                  'AWS_CONFIG_FILE': '/dev/null'}, clear=True), \
         patch.object(socket.socket, 'connect', side_effect=AssertionError('Network forbidden')):
        yield


def sam(template, values):
    with offline_processors():
        return transform(copy.deepcopy(template), dict(values), NoManagedPolicies())


def language(template, values):
    with offline_processors():
        return expand_language_extensions(copy.deepcopy(template), dict(values), enabled=True).expanded_template


def active_graph(template, values):
    selected = p.conditions(template, values)
    return {'resources': p.resources(template, values),
            'outputs': p.resolve(template['Outputs'], values, selected)}


class SamTransform(unittest.TestCase):
    def test_language_extensions_must_precede_sam(self):
        self.assertEqual(TRANSFORMS, p.load_template(TEMPLATE)['Transform'])

    def test_both_processors_deny_network_and_inherited_aws_context(self):
        def network_attempt(*args, **kwargs):
            self.assertNotIn('AWS_PROFILE', os.environ)
            self.assertNotIn('AWS_ACCESS_KEY_ID', os.environ)
            self.assertEqual('/dev/null', os.environ['AWS_SHARED_CREDENTIALS_FILE'])
            self.assertEqual('/dev/null', os.environ['AWS_CONFIG_FILE'])
            with socket.socket() as client:
                client.connect(('example.invalid', 443))
        for processor, entrypoint in [(sam, 'transform'), (language, 'expand_language_extensions')]:
            with self.subTest(processor=entrypoint), \
                 patch.dict(os.environ, {'AWS_PROFILE': 'default', 'AWS_ACCESS_KEY_ID': 'test-fixture'}), \
                 patch(__name__ + '.' + entrypoint, side_effect=network_attempt):
                with self.assertRaisesRegex(AssertionError, 'Network forbidden'):
                    processor({}, {})

    def test_scalar_parameter_code_uri_reproduces_all_eight_released_failures(self):
        template = p.load_template(TEMPLATE)
        template['Transform'] = 'AWS::Serverless-2016-10-31'
        template['Resources']['HttpFunction']['Properties']['CodeUri'] = p.defaults(template)['HttpArtifactUri']
        for name in SAM_FUNCTIONS:
            template['Resources'][name]['Properties']['CodeUri'] = {'Ref': 'ActionArtifactUri'}
        values = fixture()[0] | {'ActionArtifactUri': URI}
        with self.assertRaises(InvalidDocumentException) as rejected:
            sam(template, values)
        failures = rejected.exception.causes
        self.assertEqual(SAM_FUNCTIONS, {failure._logical_id for failure in failures})
        self.assertTrue(all('requires Bucket and Key' in str(failure) for failure in failures))

    def test_wrapped_code_uri_still_requires_language_extensions(self):
        template = p.load_template(TEMPLATE)
        template['Transform'] = 'AWS::Serverless-2016-10-31'
        with self.assertRaises(InvalidDocumentException) as rejected:
            sam(template, fixture()[0] | {'ActionArtifactUri': URI})
        self.assertEqual(SAM_FUNCTIONS | {'HttpFunction'}, {failure._logical_id for failure in rejected.exception.causes})

    def test_transform_only_is_insufficient_for_disabled_transition_function(self):
        template = p.load_template(TEMPLATE)
        for name in SAM_FUNCTIONS:
            template['Resources'][name]['Properties']['CodeUri'] = {'Ref': 'ActionArtifactUri'}
        values = fixture()[0] | {'ActionArtifactUri': URI, 'TransitionWorkerPlacement': 'COMPUTE'}
        expanded = language(template, values)
        self.assertNotIn('CodeUri', expanded['Resources']['TransitionWorkerFunction']['Properties'])
        with self.assertRaises(InvalidDocumentException) as rejected:
            sam(expanded, values)
        self.assertEqual({'TransitionWorkerFunction'},
                         {failure._logical_id for failure in rejected.exception.causes})
        self.assertIn("Only one of 'InlineCode' or 'CodeUri' can be set", str(rejected.exception))

    def test_restricted_retained_transform_preserves_recovery_without_paid_alarm_resources(self):
        template = p.load_template(TEMPLATE)
        values, inventory, guard = fixture('RESTRICTED_PILOT')
        values.update(ReleaseIsolationMode='RETAINED_RELEASE_VERSIONS',
                      RetainedReleaseValidation='validated', WorkEntryId='work',
                      TransitionEntryId='transition', AwaitCompletionEntryId='await', ActionEntryId='action')
        values = p.check(template, values, inventory, guard, account_id='123456789012')
        values.update({'ActionArtifactUri': URI, 'AWS::Region': 'us-east-2',
                       'AWS::AccountId': '123456789012', 'AWS::StackName': 'retained-low-cost'})
        transformed = sam(language(template, values), values)
        graph = active_graph(transformed, values)['resources']
        types = [resource['Type'] for resource in graph.values()]
        self.assertEqual(0, types.count('AWS::CloudWatch::Alarm'))
        self.assertEqual(0, types.count('AWS::SecretsManager::Secret'))
        self.assertEqual(12, types.count('AWS::SQS::Queue'))
        self.assertEqual(10, types.count('AWS::Lambda::Function'))
        self.assertEqual(2, types.count('AWS::Events::Rule'))
        self.assertIn('RetainedReleaseRegistryReadOnlyPolicy', graph)
        self.assertIn('RetainedWorkerInvocationPolicy', graph)
        self.assertIn('RetainedActionInvocationPolicy', graph)
        for name in SAM_FUNCTIONS:
            env = graph[name]['Properties']['Environment']['Variables']
            self.assertEqual('SSM_STANDARD', env['TPF_AWS_DURABLE_SIGNING_SOURCE'])
            self.assertNotIn('TPF_AWS_DURABLE_WORKER_SECRET_ARN', env)
        iam = [resource for resource in graph.values()
               if resource['Type'] in ('AWS::IAM::Role', 'AWS::IAM::Policy')]
        self.assertNotIn('secretsmanager:GetSecretValue', str(iam))

    def test_in_process_retained_transform_has_no_remote_consumer(self):
        template = p.load_template(TEMPLATE)
        values, inventory, guard = fixture('RESTRICTED_PILOT')
        values.update(ReleaseIsolationMode='RETAINED_RELEASE_VERSIONS',
                      RetainedReleaseValidation='validated', WorkEntryId='work',
                      TransitionEntryId='', AwaitCompletionEntryId='await', ActionEntryId='action',
                      TransitionWorkerPlacement='IN_PROCESS')
        values = p.check(template, values, inventory, guard, account_id='123456789012')
        values.update({'ActionArtifactUri': URI, 'AWS::Region': 'us-east-2',
                       'AWS::AccountId': '123456789012', 'AWS::StackName': 'retained-in-process'})
        graph = active_graph(sam(language(template, values), values), values)['resources']
        types = [resource['Type'] for resource in graph.values()]
        self.assertEqual(9, types.count('AWS::Lambda::Function'))
        self.assertEqual(9, types.count('AWS::SQS::Queue'))
        self.assertEqual(6, types.count('AWS::Lambda::EventSourceMapping'))
        self.assertEqual(2, types.count('AWS::Events::Rule'))
        self.assertEqual(0, types.count('AWS::CloudWatch::Alarm'))
        self.assertIn('WorkFunction', graph)
        self.assertNotIn('TransitionWorkerFunction', graph)

    def test_all_sixteen_variants_expand_with_full_code_and_recovery_continuity(self):
        template = p.load_template(TEMPLATE)
        for profile, signing, placement, boundary in itertools.product(
                ['HARDENED', 'RESTRICTED_PILOT'], ['SECRETS_MANAGER', 'SSM_STANDARD'],
                ['FUNCTION', 'COMPUTE'], ['', 'arn:aws:iam::123456789012:policy/runtime-boundary']):
            with self.subTest(profile=profile, signing=signing, placement=placement, boundary=boundary):
                values = fixture(profile)[0] | {'SigningSource': signing,
                    'TransitionWorkerPlacement': placement, 'RuntimePermissionsBoundaryArn': boundary,
                    'ActionArtifactUri': URI, 'DurableArtifactBucket': 'tpf-raw-host-test',
                    'DurableArtifactKey': 'retained/raw-host.jar', 'AWS::Region': 'us-east-2',
                    'AWS::AccountId': '123456789012', 'AWS::StackName': 'tpf-native-test'}
                expanded = language(template, values)
                for name in SAM_FUNCTIONS:
                    self.assertEqual(URI, expanded['Resources'][name]['Properties']['CodeUri'])
                translated = sam(expanded, values)
                self.assertFalse(any(r['Type'].startswith('AWS::Serverless::')
                                     for r in translated['Resources'].values()))
                self.assertEqual('HttpIngressSelected', translated['Resources']['HttpFunction']['Condition'])
                functions = {n: r for n, r in translated['Resources'].items()
                             if r['Type'] == 'AWS::Lambda::Function' and n != 'HttpFunction'}
                self.assertEqual(SAM_FUNCTIONS | {'DurableFunction', 'IngressFunction'}, set(functions))
                for name, resource in functions.items():
                    props = resource['Properties']
                    self.assertEqual('java21', props['Runtime'], name)
                    self.assertEqual(['arm64'], props['Architectures'], name)
                    expected = ({'S3Bucket': BUCKET, 'S3Key': KEY} if name in SAM_FUNCTIONS else
                                {'S3Bucket': 'tpf-raw-host-test', 'S3Key': 'retained/raw-host.jar'})
                    self.assertEqual(expected, props['Code'], name)
                # Independent reference: the previous SAM-only template with only
                # its eight intended CodeUri parameter values supplied literally.
                # Apply the same official parameter resolution AFTER its SAM pass.
                intended = copy.deepcopy(template)
                intended['Transform'] = 'AWS::Serverless-2016-10-31'
                intended['Resources']['HttpFunction']['Properties']['CodeUri'] = values['HttpArtifactUri']
                for name in SAM_FUNCTIONS:
                    intended['Resources'][name]['Properties']['CodeUri'] = URI
                reference = sam(intended, values)
                reference['Transform'] = TRANSFORMS
                reference = language(reference, values)
                self.assertEqual(active_graph(reference, values), active_graph(translated, values))
                self.assertEqual(87, len(translated['Parameters']))
                self.assertEqual('', translated['Parameters']['CommandEffectTable']['Default'])
                self.assertEqual('', translated['Parameters']['RetainedDurableVersionArns']['Default'])
                self.assertEqual(template['Parameters'], translated['Parameters'])
                self.assertEqual(template['Rules'], translated['Rules'])
                self.assertEqual(15, len(translated['Outputs']))

    def test_private_http_adds_one_retained_function_without_a_new_ingress_surface(self):
        template = p.load_template(TEMPLATE)
        values, inventory, guard = fixture('RESTRICTED_PILOT')
        http_uri = 's3://' + BUCKET + '/tpf/releases/http.zip'
        http_digest = 'b' * 43 + '='
        old = 'arn:aws:lambda:us-east-2:123456789012:function:retained-low-cost-durable:7'
        values.update(ReleaseIsolationMode='RETAINED_RELEASE_VERSIONS',
                      RetainedReleaseValidation='validated', WorkEntryId='work',
                      TransitionEntryId='transition', AwaitCompletionEntryId='await', ActionEntryId='action',
                      RetainedDurableVersionArns=[old], HttpArtifactUri=http_uri, HttpCodeSha256=http_digest,
                      HttpEntryId='private-http', ActionArtifactUri=URI)
        values = p.check(template, values, inventory, guard, account_id='123456789012')
        values.update({'AWS::Region': 'us-east-2', 'AWS::AccountId': '123456789012',
                       'AWS::StackName': 'retained-low-cost'})
        graph = active_graph(sam(language(template, values), values), values)['resources']
        types = [resource['Type'] for resource in graph.values()]
        self.assertEqual(11, types.count('AWS::Lambda::Function'))
        self.assertEqual(11, types.count('AWS::Lambda::Version'))
        self.assertEqual(11, types.count('AWS::Logs::LogGroup'))
        self.assertEqual(0, types.count('AWS::CloudWatch::Alarm'))
        self.assertEqual(0, types.count('AWS::SecretsManager::Secret'))
        self.assertEqual(2, types.count('AWS::Events::Rule'))
        self.assertEqual(12, types.count('AWS::SQS::Queue'))
        self.assertTrue(all(kind not in ('AWS::Lambda::Url', 'AWS::ApiGateway::RestApi', 'AWS::ApiGatewayV2::Api')
                            for kind in types))
        self.assertFalse(any('HttpFunction' in str(resource['Properties']) for resource in graph.values()
                             if resource['Type'] in ('AWS::Lambda::Permission', 'AWS::Lambda::EventSourceMapping')))
        http = graph['HttpFunction']['Properties']
        self.assertEqual({'S3Bucket': BUCKET, 'S3Key': 'tpf/releases/http.zip'}, http['Code'])
        self.assertEqual('io.quarkus.amazon.lambda.runtime.QuarkusStreamHandler::handleRequest', http['Handler'])
        self.assertEqual(2, http['ReservedConcurrentExecutions'])
        env = http['Environment']['Variables']
        self.assertEqual('true', env['TPF_AWS_DURABLE_HTTP_ENABLED'])
        self.assertEqual('dynamo', env['PIPELINE_AWAIT_ADMISSION_STORE'])
        self.assertEqual('SSM_STANDARD', env['TPF_AWS_DURABLE_SIGNING_SOURCE'])
        self.assertNotIn('QUARKUS_LAMBDA_HANDLER', env)
        self.assertNotIn('TPF_AWS_DURABLE_RESUME_TOKEN_SECRET_ARN', env)
        self.assertEqual(http_digest, graph['HttpVersion']['Properties']['CodeSha256'])
        for role in ('HttpFunction', 'HttpVersion', 'HttpRole', 'HttpFunctionLogs'):
            self.assertEqual('Retain', graph[role]['DeletionPolicy'])
            self.assertEqual('Retain', graph[role]['UpdateReplacePolicy'])
        statements = graph['HttpRole']['Properties']['Policies'][0]['PolicyDocument']['Statement']
        permissions = [statement['Resource'] for statement in statements if
                       statement['Action'] == ['lambda:GetFunctionConfiguration', 'lambda:InvokeFunction']]
        self.assertEqual([{'Ref': 'DurableVersion'}, [old]], permissions)


if __name__ == '__main__':
    unittest.main()
