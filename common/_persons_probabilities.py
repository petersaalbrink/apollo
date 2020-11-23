from functools import lru_cache, partial
from itertools import combinations
from typing import Any, Dict, Tuple

from .connectors.mx_elastic import ESClient
from .exceptions import PersonsError

es_initials = partial(
    ESClient("cdqc.person_data_initials_occurrence", index_exists=True).find,
    size=1, source_only=True, _source="proportion",
)
es_lastnames = partial(
    ESClient("cdqc.person_data_lastname_occurrence", index_exists=True).find,
    size=1, source_only=True, _source=["regular.proportion", "fuzzy.proportion"],
)


class Constant:
    alpha = .05
    adults_nl = 14_000_000
    yearly_deceased = 151_885
    population_size = adults_nl + (yearly_deceased * 20)
    db_count = ESClient("cdqc.person_data", index_exists=True).count()
    max_proportion_initials = es_initials({"sort": {"proportion": "desc"}})["proportion"]
    max_proportion_lastname = es_lastnames({"sort": {"regular.proportion": "desc"}})["regular"]["proportion"]
    mean_proportion_lastname = (max_proportion_lastname + es_lastnames(
        {"sort": {"regular.proportion": "asc"}})["regular"]["proportion"]) / 2
    max_age = 90
    min_age = 18
    dob_fp = 1 / (365.25 * (max_age - min_age))
    number_of_residential_addresses = 7_088_757
    number_of_postcodes = 437_383
    yearly_movements = 1_700_000
    average_lifespan = 82
    inhabitants_nl = 17_461_543
    addresses_per_person = ((yearly_movements * average_lifespan) / inhabitants_nl) + 2
    address_fp = (addresses_per_person ** 2) / number_of_residential_addresses
    postcode_fp = (addresses_per_person ** 2) / number_of_postcodes
    mobile_total_distributed = 55_000_000
    mobile_yearly_reused = 100_000
    phone_number_of_years = 10
    mobile_reuse_p = (mobile_yearly_reused * phone_number_of_years) / mobile_total_distributed
    mobile_fp = (1 / 2) * mobile_reuse_p
    landline_total_distributed = 70_000_000
    landline_yearly_reused = 400_000
    landline_reuse_p = (landline_yearly_reused * phone_number_of_years) / landline_total_distributed
    landline_fp = (1 / 2) * landline_reuse_p


def set_alpha(alpha: float = .05) -> bool:
    Constant.alpha = alpha
    return True


def set_population_size(oldest_client_record_in_years: int = 20) -> bool:
    Constant.population_size = Constant.adults_nl + (Constant.yearly_deceased * oldest_client_record_in_years)
    return True


def set_number_of_residential_addresses() -> bool:
    Constant.number_of_residential_addresses = ESClient(
        "real_estate_alias", host="prod", index_exists=True).distinct_count(
        field="address.identification.addressId",
        find={"match": {"houseDetails.usePurpose": "woonfunctie"}})
    return True


def set_number_of_postcodes() -> bool:
    Constant.number_of_postcodes = ESClient(
        "real_estate_alias", host="prod", index_exists=True).distinct_count(
        field="address.identification.postalCode",
        find={"match": {"houseDetails.usePurpose": "woonfunctie"}})
    return True


@lru_cache()
def proportion_lastname(lastname: str) -> Dict[str, float]:
    if not lastname:
        return {"regular": Constant.mean_proportion_lastname, "fuzzy": Constant.mean_proportion_lastname}
    count = es_lastnames({"query": {"term": {"lastname.keyword": lastname}}})
    try:
        return {"regular": count["regular"]["proportion"], "fuzzy": count["fuzzy"]["proportion"]}
    except TypeError:
        return {"regular": Constant.mean_proportion_lastname, "fuzzy": Constant.mean_proportion_lastname}


@lru_cache()
def proportion_initial(initial: str) -> float:
    count = es_initials({"query": {"term": {"initials.keyword": initial}}})
    try:
        return count["proportion"]
    except TypeError:
        return Constant.max_proportion_initials


@lru_cache()
def get_proportions_lastname(lastname: str) -> Dict[str, Dict[str, float]]:
    """Get counts for all parts of the lastname."""
    if lastname:
        names = set(lastname.split())
        names.add(lastname)
        return {name: proportion_lastname(name) for name in names}
    else:
        return {"": {"": Constant.max_proportion_lastname}}


@lru_cache()
def get_proportions_initials(initials: str) -> Dict[str, float]:
    if initials:
        return {initial: proportion_initial(initial) for initial in {initials[0], initials}}
    else:
        return {"": Constant.max_proportion_initials}


@lru_cache()
def estimated_people_with_lastname(lastname: str):
    return proportion_lastname(lastname)["regular"] * Constant.population_size


@lru_cache()
def base_calculations(lastname: str, initials: str) -> Dict[Tuple[str, ...], float]:
    lastname_proportions = get_proportions_lastname(lastname)
    initials_proportions = get_proportions_initials(initials)
    return {
        (name, initial, situation): Constant.population_size * l_proportion * i_proportion
        for name, l_proportions in lastname_proportions.items()
        for situation, l_proportion in l_proportions.items()
        for initial, i_proportion in initials_proportions.items()
    }


@lru_cache()
def full_calculation_fp(
        lastname: str,
        initials: str,
        date_of_birth: bool = None,
        address: bool = None,
        postcode: bool = None,
        mobile: bool = None,
        number: bool = None,
) -> Dict[Tuple[str, ...], float]:
    if postcode and address:
        raise PersonsError("Choose postcode or full address.")
    return {
        base: p * (Constant.dob_fp if date_of_birth else 1)
                * (Constant.address_fp if address else 1)
                * (Constant.postcode_fp if postcode else 1)
                * (Constant.mobile_fp if mobile else 1)
                * (Constant.landline_fp if number else 1)
        for base, p in base_calculations(lastname, initials).items()
    }


@lru_cache()
def extra_fields_calculation(lastname: str, initials: str, **kwargs: Any) -> Dict[Tuple[str, ...], float]:
    """Calculate which combinations of extra fields are valid.

    Comments:
    # query_builder should check if the base_calculation_fp < .05,
    # if not check which combination of extra fields will do so.
    # add all possible combinations as OR statements.

    Example:
    print(extra_fields_calculation("Saalbrink", "PP", mobile=True, date_of_birth=True, address=True))
    """
    lastname = lastname or None
    initials = initials or ""
    try:
        bases = base_calculations(lastname, initials)
    except Exception as e:
        raise PersonsError("No calculation possible") from e

    if all(p_fp < Constant.alpha for p_fp in bases.values()):
        return bases

    kws = [kw for kw, arg in kwargs.items() if arg]
    kws_combinations = [
        {kw: True for kw in kw_combi}
        for i in range(1, len(kws) + 1)
        for kw_combi in combinations(kws, i)
    ]

    calculations = {}
    valid_combinations = set()
    for kws in kws_combinations:
        for base, p_fp in full_calculation_fp(lastname, initials, **kws).items():
            if bases[base] < Constant.alpha:
                # The chance from the base calculation is already valid
                valid_combinations.add(base)
                calculations[base] = bases[base]
            elif p_fp < Constant.alpha:
                # The extra fields made the probability significant
                if not any((
                        (*base, *v) in valid_combinations
                        for i in range(1, len(kws) + 1)
                        for v in combinations(kws, i)
                )):
                    # We haven't seen an easier valid option
                    valid = (*base, *kws)
                    valid_combinations.add(valid)
                    calculations[valid] = p_fp

    if not calculations:
        raise PersonsError("No valid combinations")

    return calculations
