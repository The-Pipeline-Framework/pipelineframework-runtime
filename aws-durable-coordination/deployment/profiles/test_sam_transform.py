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
        self.assertEqual(SAM_FUNCTIONS, {failure._logical_id for failure in rejected.exception.causes})

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
                functions = {n: r for n, r in translated['Resources'].items()
                             if r['Type'] == 'AWS::Lambda::Function'}
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
                for name in SAM_FUNCTIONS:
                    intended['Resources'][name]['Properties']['CodeUri'] = URI
                reference = sam(intended, values)
                reference['Transform'] = TRANSFORMS
                reference = language(reference, values)
                self.assertEqual(active_graph(reference, values), active_graph(translated, values))
                self.assertEqual(74, len(translated['Parameters']))
                self.assertEqual(template['Parameters'], translated['Parameters'])
                self.assertEqual(template['Rules'], translated['Rules'])
                self.assertEqual(14, len(translated['Outputs']))


if __name__ == '__main__':
    unittest.main()
