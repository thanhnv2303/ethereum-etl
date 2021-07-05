import pymongo

if __name__ == '__main__':
    NAME = "just_for_dev"
    PASSWORD = "password_for_dev"
    HOST = "25.39.155.190"
    PORT = "27037"
    url = f"mongodb://{NAME}:{PASSWORD}@{HOST}:{PORT}"
    client = pymongo.MongoClient(url)
    d = client["extract_data_knowledge_graph"]
    collections = d.collection_names()
    for collection_name in collections:
        collection = d[collection_name]
        if "block_number_increase" not in collection.index_information():
            collection.create_index([("block_number", 1)], name="block_number_increase", background=True)
        if "block_number_hashed" not in collection.index_information():
            collection.create_index([("block_number", "hashed")], name="block_number_hashed", background=True)
        try:
            collection.drop_index(index_or_name="block_num_hashed")
        except Exception as e:
            print(e)
