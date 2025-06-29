from typing import Optional


class UCGID:
    _SUMMARY_LEVEL_STATE = "040"
    _SUMMARY_LEVEL_COUNTY = "050"
    _SUMMARY_LEVEL_ZCTA = "860"
    _SEPARATOR = "0000US"

    def __init__(
        self,
        state_fips: Optional[str] = None,
        county_fips: Optional[str] = None,
        zcta_code: Optional[str] = None,
        instance: Optional[str] = None,
    ):
        if zcta_code and (state_fips or county_fips):
            raise ValueError("Cannot specify ZCTA with state or county FIPS codes.")
        if county_fips and not state_fips:
            raise ValueError("County FIPS must be provided along with a state FIPS.")

        self._state_fips = state_fips
        self._county_fips = county_fips
        self._zcta_code = zcta_code
        self.instance = instance

    @classmethod
    def from_state(cls, state_fips: str) -> "UCGID":
        return UCGID(state_fips=state_fips, instance="state")

    @classmethod
    def from_county(cls, state_fips: str, county_fips: str) -> "UCGID":
        return UCGID(state_fips=state_fips, county_fips=county_fips, instance="county")

    @classmethod
    def from_zcta(cls, zcta_code: str) -> "UCGID":
        return UCGID(zcta_code=zcta_code, instance="zcta")

    def __str__(self) -> str:
        if self._zcta_code:
            return f"{self._SUMMARY_LEVEL_ZCTA}{self._SEPARATOR}{self._zcta_code}"
        elif self._state_fips and self._county_fips:
            return f"{self._SUMMARY_LEVEL_COUNTY}{self._SEPARATOR}{self._state_fips}{self._county_fips}"
        elif self._state_fips:
            return f"{self._SUMMARY_LEVEL_STATE}{self._SEPARATOR}{self._state_fips}"
        else:
            raise ValueError(
                "UCGID object is not configured with valid geographic codes."
            )
