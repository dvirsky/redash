import redis
from redash.query_runner import *
import json
type_mapping = {
    'TEXT': TYPE_STRING,
    'NUMERIC': TYPE_FLOAT,
    'TAG': TYPE_STRING,
    'GEO': TYPE_STRING
}


class RediSearch(BaseQueryRunner):

    @classmethod
    def configuration_schema(cls):
        return {
            'type': 'object',
            'properties': {
                'host': {
                    'type': 'string',
                    'title': 'Redis Host',
                    'default': 'localhost'
                },
                'port': {
                    'type': 'number',
                    'default': 6379,
                }
            }
        }

    @classmethod
    def annotate_query(cls):
        return False

    def test_connection(self):
        pass

    def extract_schema(self, row):
        schema = []
        for n in xrange(0, len(row), 2):
            t = TYPE_STRING
            try:
                f = float(row[n + 1])
                t = TYPE_FLOAT
            except Exception:
                pass

            schema.append(
                {'name': row[n], 'friendly_name': row[n], 'type': t})
        return schema

    def run_query(self, query, user):
        conn = redis.Redis(host=self.configuration.get(
            "host", 'localhost'), port=self.configuration.get("port", 6379))
        ret = None
        error = None
        try:

            # todo - support real splitting
            cmd = filter(None, query.strip().split())
            # print(cmd)
            response = conn.execute_command(*cmd)

            if not response:
                error = "Got empty response"
            ret = {}
            rows = []
            for i, row in enumerate(response):
                if i == 0:
                    continue

                if i == 1:
                    ret['columns'] = self.extract_schema(row)

                current = {}
                for n in xrange(0, len(row), 2):
                    current[row[n]] = row[n + 1]
                rows.append(current)
            ret['rows'] = rows

            ret = json.dumps(ret)
            print ret

        except redis.RedisError as e:
            error = "Error running query: {}".format(e)
        except KeyboardInterrupt:
            error = "Query cancelled by user."

        return ret, error

register(RediSearch)
