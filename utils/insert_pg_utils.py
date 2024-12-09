from psycopg2.extensions import AsIs

class InsertPgUtils():
    @classmethod
    def generate_query_insert_list_dict(self, insert_statement: str, data_list: list) -> tuple:
        columns = data_list[0].keys()
        values = [tuple(d[column] for column in columns) for d in data_list]
        placeholders = ', '.join(['%s'] * len(values))
        final_query = insert_statement % (AsIs(','.join(columns)), placeholders)
        return final_query, tuple(values)