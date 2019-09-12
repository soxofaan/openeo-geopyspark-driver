import re
from unittest import skip, TestCase

from openeogeotrellis.layercatalog import LayerCatalog


class TestLayerCatalog(TestCase):

    @skip("Depends on VITO infrastructure")
    def testRetrieveAllLayers(self):
        catalog = LayerCatalog()
        layers = catalog.layers()
        print(layers)


def test_layercatalog_json():
    for layer in LayerCatalog().catalog.values():
        assert re.match(r'^[A-Za-z0-9_\-\.~\/]+$', layer['id'])
        # TODO enable these other checks too
        # assert 'stac_version' in layer
        # assert 'description' in layer
        # assert 'license' in layer
        # assert 'extent' in layer


def test_issue77_band_metadata():
    for layer in LayerCatalog().catalog.values():
        # print(layer['id'])
        # TODO: stop doing this non-standard band metadata ("bands" item in metadata root)
        old_bands = [b if isinstance(b, str) else b["band_id"] for b in layer.get("bands", [])]
        eo_bands = [b["name"] for b in layer.get("properties", {}).get('eo:bands', [])]
        cube_dimension_bands = []
        for cube_dim in layer.get("properties", {}).get("cube:dimensions", {}).values():
            if cube_dim["type"] == "bands":
                cube_dimension_bands = cube_dim["values"]
        if len(old_bands) > 1:
            assert old_bands == eo_bands
            assert old_bands == cube_dimension_bands
        assert eo_bands == cube_dimension_bands
