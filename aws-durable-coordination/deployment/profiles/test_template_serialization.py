"""CloudFormation serialization and full semantic continuity of runtime66 deployment asset."""
import hashlib
import itertools
import json
from pathlib import Path
import unittest
import yaml
import preflight as p
from test_profiles import fixture, TEMPLATE

# Captured from merged runtime66 dda35144f50f214bbf663352bcb591401b8c9a37.
PARSED_TEMPLATE_SHA256 = '2df7b40cacdff91f15130cb417a8f9d84c5e5a375ccb9b873886707ea91e5c1b'
RESOLVED_GRAPH_SHA256 = {'HARDENED/SECRETS_MANAGER/FUNCTION/NO_BOUNDARY': '884892defa08d7e08497b4e8dad88f2ba3f2151d877496543ec6640624a8198a',
 'HARDENED/SECRETS_MANAGER/FUNCTION/BOUNDARY': 'a6ba05bdeb823c7e8f2caf0e171393d83f84abc83e54e655f902784bd4530fa8',
 'HARDENED/SECRETS_MANAGER/COMPUTE/NO_BOUNDARY': '4edef9eee0ceb2d5583f7d0889bc3e1e69930f2cdb6003e092bce49f105dd752',
 'HARDENED/SECRETS_MANAGER/COMPUTE/BOUNDARY': 'f042d6ab7d44a63ea218f80465cc499b2f7ba1cfcba7942284f766941bc2b2c8',
 'HARDENED/SSM_STANDARD/FUNCTION/NO_BOUNDARY': '6adacdea0aadfccaa314712a9375d085aac2acfb3b0841f83c3812727e2a8173',
 'HARDENED/SSM_STANDARD/FUNCTION/BOUNDARY': '8cc6530ab46aa039e860802a5964810f75f92dfc98450f8ee3786c518b55da1e',
 'HARDENED/SSM_STANDARD/COMPUTE/NO_BOUNDARY': '0d00dd5ff1b9d4435be28b2e349775341948312942256f8cfd89aabae37dd39f',
 'HARDENED/SSM_STANDARD/COMPUTE/BOUNDARY': '6601cf2fac6d6deb369f0d74503c97bedb9e5a356fa8f718cee5c8a1ca2c55ba',
 'RESTRICTED_PILOT/SECRETS_MANAGER/FUNCTION/NO_BOUNDARY': '866efb4099e1b2e7bbcbc5475ae435d5c62ad8e56deb3a533f9972801ea4dd69',
 'RESTRICTED_PILOT/SECRETS_MANAGER/FUNCTION/BOUNDARY': 'ae2fdf6642ca80f1deabc76d1b7eb142c3a7ecca122249763bc3bf109ebbee78',
 'RESTRICTED_PILOT/SECRETS_MANAGER/COMPUTE/NO_BOUNDARY': '401272316bb52c531cfc29e6ec817433d3258182c2e47f531e70e36b76a560b6',
 'RESTRICTED_PILOT/SECRETS_MANAGER/COMPUTE/BOUNDARY': '4372c3953d9e41f2cc5f892fa8aaff279ed2be8749f7984ae48278d995b6ca8d',
 'RESTRICTED_PILOT/SSM_STANDARD/FUNCTION/NO_BOUNDARY': '94f24d3caa5141376db09080ec87e4c13346ae527868385519dce78230dd58e3',
 'RESTRICTED_PILOT/SSM_STANDARD/FUNCTION/BOUNDARY': 'acb2794d9160b09c9004e88a867e4f0552228aeda231025a623d42ace4ee8167',
 'RESTRICTED_PILOT/SSM_STANDARD/COMPUTE/NO_BOUNDARY': '6472d5bf57a655a61f4623abc31a0cc06e8748d9a313ba2cc5b809a27d5f541a',
 'RESTRICTED_PILOT/SSM_STANDARD/COMPUTE/BOUNDARY': '66604a4f6e35d9ae43b47f1c99a95e68c789fb934415d1c8da5c97819ed28915'}

def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()

class TemplateSerialization(unittest.TestCase):
    def test_cloudformation_template_has_no_alias_or_anchor_tokens(self):
        tokens = list(yaml.scan(Path(TEMPLATE).read_text()))
        self.assertFalse([token for token in tokens if isinstance(token, (yaml.AliasToken, yaml.AnchorToken))],
                         'CloudFormation rejects YAML aliases; publish literal resource structures')

    def test_no_yaml_merge_keys(self):
        def check(node):
            if isinstance(node, yaml.MappingNode):
                for key, value in node.value:
                    self.assertNotEqual('tag:yaml.org,2002:merge', key.tag)
                    self.assertNotEqual('<<', key.value)
                    check(value)
            elif isinstance(node, yaml.SequenceNode):
                for value in node.value: check(value)
        check(yaml.compose(Path(TEMPLATE).read_text()))

    def test_complete_parsed_contract_equals_merged_runtime66(self):
        # Includes all74 Parameters, Rules, Conditions, Globals, resources, properties and Outputs.
        self.assertEqual(PARSED_TEMPLATE_SHA256, digest(p.load_template(TEMPLATE)))

    def test_all_profile_signing_placement_boundary_graphs_equal_merged_runtime66(self):
        template = p.load_template(TEMPLATE)
        for profile, signing, placement, boundary in itertools.product(
                ['HARDENED', 'RESTRICTED_PILOT'], ['SECRETS_MANAGER', 'SSM_STANDARD'],
                ['FUNCTION', 'COMPUTE'], ['', 'arn:aws:iam::123456789012:policy/runtime-boundary']):
            key = '/'.join([profile, signing, placement, 'BOUNDARY' if boundary else 'NO_BOUNDARY'])
            values = fixture(profile)[0] | {'SigningSource': signing, 'TransitionWorkerPlacement': placement,
                                           'RuntimePermissionsBoundaryArn': boundary}
            selected = p.conditions(template, values)
            graph = {'conditions': selected, 'resources': p.resources(template, values),
                     'outputs': p.resolve(template['Outputs'], values, selected)}
            with self.subTest(configuration=key):
                self.assertEqual(RESOLVED_GRAPH_SHA256[key], digest(graph))

if __name__ == '__main__': unittest.main()
