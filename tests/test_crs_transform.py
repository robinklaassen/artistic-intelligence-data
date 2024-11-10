import pytest

from aid.constants import WGS84_TO_RDNEW


class TestCRSTransform:
    def test_wgs84_to_rdnew(self):
        onze_lieve_vrouwentoren = (52.155172, 5.387201)
        x, y = WGS84_TO_RDNEW.transform(*onze_lieve_vrouwentoren)
        assert x == pytest.approx(155_000, rel=1e-4)
        assert y == pytest.approx(463_000, rel=1e-4)
