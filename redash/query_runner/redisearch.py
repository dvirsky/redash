import redis
from redash.query_runner import *
import json
type_mapping = {
    'TEXT': TYPE_STRING,
    'NUMERIC': TYPE_FLOAT,
    'TAG': TYPE_STRING,
    'GEO': TYPE_STRING
}
import shlex


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
        print row
        schema = []
        for (name, t) in row:

            if t == 'number':
                t = TYPE_FLOAT
            else:  # all other are strings for now
                t = TYPE_STRING
            schema.append(
                {'name': name, 'friendly_name': name, 'type': t})
        return schema

    def format_request(self, query):
        """
        Properly split the query and add needed stuff
        """
        # split using shlex to handle quoted spaced terms
        cmd = filter(None, shlex.split(query.strip()))

        # Add "withschema" to aggregate reqeusts
        if len(cmd) > 3 and cmd[0].upper() == 'FT.AGGREGATE' and \
                not any((x.upper() == 'WITHSCHEMA' for x in cmd)):

            cmd.insert(3, "WITHSCHEMA")
        print cmd
        return cmd

    def run_query(self, query, user):
        conn = redis.Redis(host=self.configuration.get(
            "host", 'localhost'), port=self.configuration.get("port", 6379))
        ret = None
        error = None
        try:
            # todo - support real splitting
            cmd = self.format_request(query)

            try:
                response = conn.execute_command(*cmd)
            except redis.ResponseError as e:
                error = "Redis Error: %s" % e
            else:
                if not response:
                    error = "Got empty response"
                ret = {}
                rows = []
                for i, row in enumerate(response):
                    if i == 0:
                        ret['columns'] = self.extract_schema(row)
                        continue
                    elif i == 1:
                        continue

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
