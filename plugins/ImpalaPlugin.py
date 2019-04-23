from __future__ import unicode_literals

from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

try:
    basestring
except NameError:
    basestring = str

def split_sql_expressions(text):
    results = []
    current = ''
    state = None
    for c in text:
        if state is None:  # default state, outside of special entity
            current += c
            if c in '"\'':
                # quoted string
                state = c
            elif c == '-':
                # probably "--" comment
                state = '-'
            elif c == '/':
                # probably '/*' comment
                state = '/'
            elif c == ';':
                # remove it from the statement
                current = current[:-1].strip()
                # and save current stmt unless empty
                if current:
                    results.append(current)
                current = ''
        elif state == '-':
            if c != '-':
                # not a comment
                state = None
                current += c
                continue
            # remove first minus
            current = current[:-1]
            # comment until end of line
            state = '--'
        elif state == '--':
            if c == '\n':
                # end of comment
                # and we do include this newline
                current += c
                state = None
                # else just ignore
        elif state == '/':
            if c != '*':
                state = None
                current += c
                continue
            # remove starting slash
            current = current[:-1]
            # multiline comment
            state = '/*'
        elif state == '/*':
            if c == '*':
                # probably end of comment
                state = '/**'
        elif state == '/**':
            if c == '/':
                state = None
            else:
                # not an end
                state = '/*'
        elif state[0] in '"\'':
            current += c
            if state.endswith('\\'):
                # prev was backslash, don't check for ender
                # just revert to regular state
                state = state[0]
                continue
            elif c == '\\':
                # don't check next char
                state += '\\'
                continue
            elif c == state[0]:
                # end of quoted string
                state = None
        else:
            raise Exception('Illegal state %s' % state)

    if current:
        current = current.rstrip(';').strip()
        if current:
            results.append(current)

    return results

# will show up under airflow.hooks.impala_plugin.ImpalaHook
class ImpalaHook(BaseHook):
    def __init__(self, impala_conn_id='impala'):
        self.impala_conn_id = impala_conn_id

    def get_conn(self, schema=None):
        db = self.get_connection(self.impala_conn_id)

        from impala.dbapi import connect
        return connect(
            host=db.host,
            port=db.port,
            database=schema or db.schema or 'default')

    def execute(self, hql, schema='default'):
        with self.get_conn(schema) as conn:
            if isinstance(hql, basestring):
                hql = split_sql_expressions(hql)
            cur = conn.cursor()
            for statement in hql:
                cur.execute(statement)
                self.log.info(cur.get_log())


# will show up under airflow.operators.impala_plugin.ImpalaOperator
class ImpalaOperator(BaseOperator):
    """
    Executes hql code in Impala.

    :param hql: the hql to be executed. Note that you may also use
        a relative path from the dag file of a (template) hive script.
    :type hql: string
    :param impala_conn_id: reference to the Hive database
    :type impala_conn_id: string
    """

    template_fields = ('hql', 'schema', 'impala_conn_id')
    template_ext = ('.hql', '.sql',)
    ui_color = '#f0e4ec'

    @apply_defaults
    def __init__(
            self, hql,
            impala_conn_id='impala',
            schema='default',
            *args, **kwargs):
        super(ImpalaOperator, self).__init__(*args, **kwargs)
        self.hql = hql
        self.schema = schema
        self.impala_conn_id = impala_conn_id

    def get_hook(self):
        return ImpalaHook(
            impala_conn_id=self.impala_conn_id)

    def execute(self, context):
        self.log.info('Executing: %s', self.hql)
        hook = self.get_hook()
        hook.execute(hql=self.hql, schema=self.schema)


class AirflowImpalaPlugin(AirflowPlugin):
    name = "impala_plugin"
    operators = [ImpalaOperator]
    hooks = [ImpalaHook]
