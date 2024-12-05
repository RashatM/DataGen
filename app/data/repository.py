import json
from typing import List

from app.dto.mock_data import MockDataEntity, MockDataSchema
from app.interfaces.repository import IMockRepository


class MockRepository(IMockRepository):

    def get_entity_schemas(self) -> List[MockDataSchema]:
        with open("./data_gen_v1/gen_params.json") as f:
            data = json.load(f)

