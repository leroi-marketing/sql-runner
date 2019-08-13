from textwrap import dedent

from src.query import Query

class AzureDwhQuery(Query):
    @property
    def distribution(self) -> str:
        return "DISTRIBUTION = ROUND_ROBIN"

    @property
    def create_table_stmt(self) -> str:
        """ Statement that creates a table out of `select_stmt`
        """
        # https://docs.microsoft.com/en-us/sql/t-sql/statements/create-table-as-select-azure-sql-data-warehouse?view=azure-sqldw-latest
        return dedent(f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema_name}{self.schema_suffix};
        DROP TABLE IF EXISTS {self.schema_name}{self.schema_suffix}.{self.table_name} CASCADE;
        DROP TABLE IF EXISTS {self.name} CASCADE;
        CREATE TABLE {self.name}
        WITH ( {self.distribution} )
        AS
        {self.select_stmt};
        """)

    @property
    def materialize_view_stmt(self) -> str:
        """ Statement that creates a materialized view, out of a `select_stmt`
        """
        return dedent(f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema_prefix}{self.schema_name}{self.schema_suffix};
        DROP VIEW IF EXISTS {self.schema_prefix}{self.schema_name}{self.schema_suffix}.{self.table_name} CASCADE;
        CREATE MATERIALIZED VIEW {self.schema_prefix}{self.schema_name}{self.schema_suffix}.{self.table_name}
        WITH ( {self.distribution} )
        AS
        {self.select_stmt};
        """)
