import re

from app.config import iRODSConfig

from irods.column import Like, In
from irods.models import Collection, DataObjectMeta

SEARCHFIELD = [
    "TITLE", "SUBTITLE", "CONTRIBUTOR"
]


class Search:
    def search_filter_relation(self, item):
        # Since only search result has order, we need to update item.filter value based on search result
        # The relationship between search and filter will always be AND
        if item.filter != {}:
            dataset_list = []
            for dataset in item.search["submitter_id"]:
                if dataset in item.filter["submitter_id"]:
                    dataset_list.append(dataset)
            item.filter["submitter_id"] = dataset_list
        else:
            item.filter["submitter_id"] = item.search["submitter_id"]

    def generate_dataset_list(self, SESSION, keyword_list):
        query = SESSION.query(Collection.name, DataObjectMeta.value)
        id_dict = {}
        for keyword in keyword_list:
            query = SESSION.query(Collection.name, DataObjectMeta.value).filter(
                In(DataObjectMeta.name, SEARCHFIELD)).filter(
                Like(DataObjectMeta.value, f"%{keyword}%"))
            for result in query:
                content_list = re.findall(
                    "[a-zA-Z0-9]+", result[DataObjectMeta.value])
                if keyword in content_list:
                    dataset = re.sub(
                        f"{iRODSConfig.IRODS_ENDPOINT_URL}/", "", result[Collection.name])
                    if dataset not in id_dict.keys():
                        id_dict[dataset] = 1
                    else:
                        id_dict[dataset] += 1
        return sorted(id_dict, key=id_dict.get, reverse=True)