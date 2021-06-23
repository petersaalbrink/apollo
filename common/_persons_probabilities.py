from __future__ import annotations

from collections import namedtuple
from functools import lru_cache, partial
from itertools import combinations
from typing import Any

from .connectors.mx_elastic import ESClient
from .exceptions import PersonsError

es_initials = partial(
    ESClient("cdqc.person_data_initials_occurrence").find,
    size=1,
    source_only=True,
    _source="proportion",
)
es_lastnames = partial(
    ESClient("cdqc.person_data_lastname_occurrence").find,
    size=1,
    source_only=True,
    _source=["regular", "fuzzy"],
)
es_firstnames = partial(
    ESClient("cdqc.person_data_firstname_occurrence").find,
    size=1,
    source_only=True,
    _source="count",
)
NameCounts = namedtuple("NameCounts", ("first", "last"))


class Constant:
    alpha = 0.05
    adults_nl = 14_000_000
    yearly_deceased = 151_885
    population_size = adults_nl + (yearly_deceased * 20)
    with ESClient("cdqc.person_data") as _es:
        db_count = _es.count()
    _result = es_initials({"sort": {"proportion": "desc"}})
    assert isinstance(_result, dict)
    max_proportion_initials: float = _result["proportion"]
    _result = es_lastnames({"sort": {"regular.proportion": "desc"}})
    assert isinstance(_result, dict)
    max_proportion_lastname: float = _result["regular"]["proportion"]
    _result = es_lastnames({"sort": {"regular.proportion": "asc"}})
    assert isinstance(_result, dict)
    mean_proportion_lastname = (
        max_proportion_lastname + _result["regular"]["proportion"]
    ) / 2
    del _result
    max_age = 90
    min_age = 18
    dob_fp = 1 / (365.25 * (max_age - min_age))
    number_of_residential_addresses = 7_088_757
    number_of_postcodes = 437_383
    yearly_movements = 1_700_000
    average_lifespan = 82
    inhabitants_nl = 17_461_543
    addresses_per_person = ((yearly_movements * average_lifespan) / inhabitants_nl) + 2
    fuzzy_addresses_per_person = addresses_per_person * 44
    address_fp = (addresses_per_person ** 2) / number_of_residential_addresses
    fuzzy_address_fp = (
        fuzzy_addresses_per_person ** 2
    ) / number_of_residential_addresses
    postcode_fp = (addresses_per_person ** 2) / number_of_postcodes
    mobile_total_distributed = 55_000_000
    mobile_yearly_reused = 100_000
    phone_number_of_years = 10
    mobile_reuse_p = (
        mobile_yearly_reused * phone_number_of_years
    ) / mobile_total_distributed
    mobile_fp = (1 / 2) * mobile_reuse_p
    landline_total_distributed = 70_000_000
    landline_yearly_reused = 400_000
    landline_reuse_p = (
        landline_yearly_reused * phone_number_of_years
    ) / landline_total_distributed
    landline_fp = (1 / 2) * landline_reuse_p


def set_alpha(alpha: float = 0.05) -> bool:
    Constant.alpha = alpha
    return True


def set_population_size(oldest_client_record_in_years: int = 20) -> bool:
    Constant.population_size = Constant.adults_nl + (
        Constant.yearly_deceased * oldest_client_record_in_years
    )
    return True


def set_number_of_residential_addresses() -> bool:
    Constant.number_of_residential_addresses = ESClient(
        "real_estate_alias", host="prod"
    ).distinct_count(
        field="address.identification.addressId",
        find={"match": {"houseDetails.usePurpose": "woonfunctie"}},
    )
    return True


def set_number_of_postcodes() -> bool:
    Constant.number_of_postcodes = ESClient(
        "real_estate_alias", host="prod"
    ).distinct_count(
        field="address.identification.postalCode",
        find={"match": {"houseDetails.usePurpose": "woonfunctie"}},
    )
    return True


@lru_cache
def get_es_lastname(lastname: str) -> dict[str, dict[str, float]]:
    result = es_lastnames({"query": {"term": {"lastname.keyword": lastname}}})
    assert isinstance(result, dict)
    return result


@lru_cache
def get_es_firstname(firstname: str) -> dict[str, float]:
    result = es_firstnames({"query": {"term": {"firstname.keyword": firstname}}})
    assert isinstance(result, dict)
    return result


@lru_cache
def get_name_counts(name: str) -> NameCounts:
    try:
        first = get_es_firstname(name)["count"] or 0
    except TypeError:
        first = 0
    try:
        last = get_es_lastname(name)["regular"]["count"] or 0
    except TypeError:
        last = 0
    return NameCounts(first, last)


@lru_cache
def default_proportion_lastname() -> dict[str, float]:
    return {
        "regular": Constant.mean_proportion_lastname,
        "fuzzy": Constant.mean_proportion_lastname,
    }


@lru_cache
def proportion_lastname(lastname: str) -> dict[str, float]:
    if not lastname:
        return default_proportion_lastname()
    count = get_es_lastname(lastname)
    try:
        return {
            "regular": count["regular"]["proportion"]
            or Constant.mean_proportion_lastname,
            "fuzzy": count["fuzzy"]["proportion"] or Constant.mean_proportion_lastname,
        }
    except TypeError:
        return default_proportion_lastname()


@lru_cache
def proportions_lastnames(lastnames: tuple[str, ...]) -> dict[str, float]:
    return {
        count: sum(get_es_lastname(name)[count]["count"] for name in lastnames)
        / Constant.db_count
        for count in ("regular", "fuzzy")
    }


@lru_cache
def proportion_initial(initial: str) -> float:
    try:
        result = es_initials({"query": {"term": {"initials.keyword": initial}}})
        assert isinstance(result, dict)
        return result["proportion"] or Constant.max_proportion_initials
    except TypeError:
        return Constant.max_proportion_initials


@lru_cache
def get_proportions_lastname(
    lastname: str,
) -> dict[str | tuple[str, ...], dict[str, float]]:
    """Get counts for all parts of the lastname."""
    if lastname:
        if " " in lastname:
            split = tuple(lastname.split())
            return {
                lastname: proportion_lastname(lastname),
                " ".join(reversed(split)): proportion_lastname(lastname),
                split: proportions_lastnames(split),
                **{name: proportion_lastname(name) for name in split},
            }
        else:
            return {lastname: proportion_lastname(lastname)}
    else:
        return {"": {"": Constant.max_proportion_lastname}}


@lru_cache
def get_proportions_initials(initials: str) -> dict[str, float]:
    if initials:
        return {
            initial: proportion_initial(initial) for initial in {initials[0], initials}
        }
    else:
        return {"": Constant.max_proportion_initials}


@lru_cache
def estimated_people_with_lastname(lastname: str) -> float:
    return proportion_lastname(lastname)["regular"] * Constant.population_size


@lru_cache
def base_calculations(
    lastname: str,
    initials: str,
) -> dict[tuple[str | tuple[str, ...], ...], float]:
    @lru_cache
    def is_partial(part: str | tuple[Any, ...]) -> bool:
        return " " in lastname and isinstance(part, str) and " " not in part

    return {
        (
            name,
            ("" if is_partial(name) else initial),
            situation,
        ): Constant.population_size
        * l_proportion
        * (1 if is_partial(name) else i_proportion)
        for name, l_proportions in get_proportions_lastname(lastname).items()
        for situation, l_proportion in l_proportions.items()
        for initial, i_proportion in {
            **get_proportions_initials(initials),
            **get_proportions_initials(""),
        }.items()
    }


@lru_cache
def full_calculation_fp(
    lastname: str,
    initials: str,
    date_of_birth: bool | None = None,
    fuzzy_address: bool | None = None,
    address: bool | None = None,
    postcode: bool | None = None,
    mobile: bool | None = None,
    number: bool | None = None,
) -> dict[tuple[str | tuple[str, ...], ...], float]:
    if postcode and address:
        raise PersonsError("Choose postcode or full address.")

    @lru_cache
    def not_partial(part: str | tuple[Any, ...], *_: Any) -> bool:
        return not (" " in lastname and isinstance(part, str) and " " not in part)

    return {
        base: p
        * (Constant.dob_fp if (date_of_birth and not_partial(*base)) else 1)
        * (Constant.fuzzy_address_fp if fuzzy_address else 1)
        * (Constant.address_fp if address else 1)
        * (Constant.postcode_fp if postcode else 1)
        * (Constant.mobile_fp if (mobile and not_partial(*base)) else 1)
        * (Constant.landline_fp if number else 1)
        for base, p in base_calculations(lastname, initials).items()
    }


@lru_cache
def extra_fields_calculation(
    lastname: str,
    initials: str,
    must_have_address: bool = False,
    **kwargs: Any,
) -> dict[tuple[str | tuple[str, ...], ...], float]:
    """Calculate which combinations of extra fields are valid.

    Query builder should check if the base_calculation_fp < Constant.alpha,
    if not check which combination of extra fields will do so;
    add all possible combinations as OR statements in query.

    Example::
        extra_fields = extra_fields_calculation(
            "Saalbrink",
            "PP",
            mobile=True,
            date_of_birth=True,
            address=True,
        )
    """
    initials = initials or ""
    try:
        bases = base_calculations(lastname, initials)
    except Exception as e:
        raise PersonsError("No calculation possible") from e

    if all(p_fp < Constant.alpha for p_fp in bases.values()):
        return bases

    kws_selection = [kw for kw, arg in kwargs.items() if arg]
    kws_combinations = (
        {kw: True for kw in kw_combi}
        for i in range(1, len(kws_selection) + 1)
        for kw_combi in combinations(kws_selection, i)
        if not ("address" in kw_combi and "fuzzy_address" in kw_combi)
    )
    if must_have_address:
        kws_combinations = (
            kw_combi
            for kw_combi in kws_combinations
            if "address" in kw_combi or "fuzzy_address" in kw_combi
        )

    @lru_cache
    def is_partial(part: str | tuple[Any, ...], *_: Any) -> bool:
        return " " in lastname and isinstance(part, str) and " " not in part

    calculations = {}
    valid_combinations = set()
    for kws in kws_combinations:
        for base, p_fp in full_calculation_fp(lastname, initials, **kws).items():
            if not must_have_address and bases[base] < Constant.alpha:
                # The chance from the base calculation is already valid
                valid_combinations.add(base)
                calculations[base] = bases[base]
            elif p_fp < Constant.alpha:
                # The extra fields made the probability significant
                if "fuzzy_address" in kws:
                    tmp = kws.copy()
                    tmp["address"] = tmp.pop("fuzzy_address")
                else:
                    tmp = kws
                if not any(
                    (*base, *v) in valid_combinations
                    for i in range(1, len(tmp) + 1)
                    for v in combinations(tmp, i)
                ):
                    if is_partial(*base):
                        kws.pop("mobile", None)
                        kws.pop("date_of_birth", None)
                    # We haven't seen an easier valid option
                    valid_combinations.add((*base, *tmp))
                    calculations[(*base, *kws)] = p_fp

    if not calculations:
        raise PersonsError("No valid combinations")

    return calculations
