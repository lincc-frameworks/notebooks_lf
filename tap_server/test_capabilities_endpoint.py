#!/usr/bin/env python
"""
Test script for the /capabilities endpoint.
Verifies that the endpoint returns proper TAP capabilities including language features.
"""

import unittest
import sys
import os
import xml.etree.ElementTree as ET

# Add the tap_server directory to the path
sys.path.insert(0, os.path.dirname(__file__))

from tap_server import app


class TestCapabilitiesEndpoint(unittest.TestCase):
    """Test suite for the /capabilities endpoint."""

    def setUp(self):
        """Set up test client."""
        self.client = app.test_client()
        self.client.testing = True

    def test_capabilities_endpoint_exists(self):
        """Test that the capabilities endpoint responds."""
        response = self.client.get('/capabilities')
        self.assertEqual(response.status_code, 200)
        self.assertIn('application/xml', response.content_type)

    def test_capabilities_xml_structure(self):
        """Test that the capabilities XML has the correct structure."""
        response = self.client.get('/capabilities')
        
        # Parse XML
        root = ET.fromstring(response.data)
        
        # Define namespaces
        ns = {
            'vosi': 'http://www.ivoa.net/xml/VOSICapabilities/v1.0',
            'tr': 'http://www.ivoa.net/xml/TAPRegExt/v1.0',
            'vr': 'http://www.ivoa.net/xml/VOResource/v1.0'
        }
        
        # Check root element
        self.assertEqual(root.tag, '{http://www.ivoa.net/xml/VOSICapabilities/v1.0}capabilities')
        
        # Check capability element
        capability = root.find('vosi:capability[@standardID="ivo://ivoa.net/std/TAP"]', ns)
        self.assertIsNotNone(capability, "TAP capability not found")
        
        # Check interface element
        interface = capability.find('vosi:interface', ns)
        self.assertIsNotNone(interface, "Interface element not found")
        
        # Check language element
        language = capability.find('vosi:language', ns)
        self.assertIsNotNone(language, "Language element not found")

    def test_adql_language_specified(self):
        """Test that ADQL language is specified."""
        response = self.client.get('/capabilities')
        root = ET.fromstring(response.data)
        
        ns = {'vosi': 'http://www.ivoa.net/xml/VOSICapabilities/v1.0'}
        
        # Find language element
        language = root.find('.//vosi:language', ns)
        self.assertIsNotNone(language, "Language element not found")
        
        # Check language name
        name = language.find('vosi:name', ns)
        self.assertIsNotNone(name, "Language name not found")
        self.assertEqual(name.text, 'ADQL')
        
        # Check language version
        version = language.find('vosi:version', ns)
        self.assertIsNotNone(version, "Language version not found")
        self.assertEqual(version.text, '2.0')

    def test_language_features_exist(self):
        """Test that language features are specified."""
        response = self.client.get('/capabilities')
        root = ET.fromstring(response.data)
        
        ns = {'vosi': 'http://www.ivoa.net/xml/VOSICapabilities/v1.0'}
        
        # Find languageFeatures element
        lang_features = root.find('.//vosi:languageFeatures', ns)
        self.assertIsNotNone(lang_features, "languageFeatures element not found")
        
        # Check type attribute
        self.assertEqual(
            lang_features.get('type'),
            'ivo://ivoa.net/std/TAPRegExt#features-adqlgeo',
            "languageFeatures type attribute incorrect"
        )

    def test_geometric_functions_supported(self):
        """Test that CIRCLE, POINT, CONTAINS, and POLYGON are listed as supported features."""
        response = self.client.get('/capabilities')
        root = ET.fromstring(response.data)
        
        ns = {'vosi': 'http://www.ivoa.net/xml/VOSICapabilities/v1.0'}
        
        # Find all feature elements
        features = root.findall('.//vosi:languageFeatures/vosi:feature/vosi:form', ns)
        self.assertIsNotNone(features, "No features found")
        self.assertGreater(len(features), 0, "Feature list is empty")
        
        # Extract feature names
        feature_names = [feature.text for feature in features]
        
        # Check for required geometric functions
        required_features = ['CIRCLE', 'POINT', 'CONTAINS', 'POLYGON']
        for required in required_features:
            self.assertIn(
                required, 
                feature_names,
                f"Required geometric function '{required}' not found in capabilities"
            )


if __name__ == '__main__':
    unittest.main()
