from __future__ import annotations

from apollo.connectors.mx_elastic import ESClient
from apollo.exceptions import NoMatch, PersonsError
from apollo.handlers import keep_trying
from apollo.persons import Constant, Person


def test_persons() -> None:
    def get_and_update_person() -> Person:
        doc = ESClient(Constant.PD_INDEX).find(
            {"query": {"function_score": {"random_score": {}}}}, size=1
        )
        assert isinstance(doc, dict)
        person = Person.from_doc(doc)
        person.update()
        return person

    assert keep_trying(
        function=get_and_update_person,
        exceptions=(NoMatch, PersonsError),
        timeout=300,
    )
