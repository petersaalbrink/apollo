from common.connectors.mx_elastic import ESClient
from common.exceptions import NoMatch, PersonsError
from common.handlers import keep_trying
from common.persons import Constant, Person


def test_persons():
    def get_and_update_person():
        doc = ESClient(Constant.PD_INDEX).find({"query": {"function_score": {"random_score": {}}}}, size=1)
        person = Person.from_doc(doc)
        person.update()
        return person

    assert keep_trying(
        function=get_and_update_person,
        exceptions=(NoMatch, PersonsError),
        timeout=300,
    )
