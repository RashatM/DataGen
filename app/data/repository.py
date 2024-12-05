import json
from typing import List

from app.dto.entities import MockEntity, MockSchema
from app.interfaces.repository import IMockRepository


class MockRepository(IMockRepository):

    def get_entity_schemas(self) -> List[MockSchema]:
        with open("./data_gen_v1/gen_params.json") as f:
            data = json.load(f)

