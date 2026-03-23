#!/usr/bin/env python
# coding: utf-8

# ## ntb-tovba-rest-api
# 
# New notebook

# In[ ]:


params = None
period_from = None


# In[2]:


import json
from time import sleep
from datetime import datetime, timezone
from typing import Union, List, Dict, Any

import pandas as pd
import requests
from jsonschema import validate
from delta.tables import DeltaTable

import notebookutils
from notebookutils import runtime
from notebookutils.credentials import getSecret

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, TimestampType


FACT_TBLS = ["sales", "docs", "stocks"]

#executor_run_id = runtime.context.get("currentRunId")
executor_run_id = json.loads(json.loads(params)['tech_params'])['executor_run_id']
business_datetime = datetime.fromisoformat(period_from)
processing_datetime = datetime.now(timezone.utc)
key_vault_uri = json.loads(params)["key_vault_uri"]


# In[3]:


class Decorator:
    @classmethod
    def validate_kwargs(cls, schema):
        def _external_wrapper(func):
            def _internal_wrapper(*args, **kwargs):
                validate(instance=kwargs, schema=schema)
                return func(*args, **kwargs)
            _internal_wrapper.__name__ = func.__name__
            return _internal_wrapper
        return _external_wrapper


class RegularExpression:
    GENERAL_STRING = r"^[A-Za-z_\s]+$"
    DATE = r"^[0-9]{4}((-)[0-9]{2}){2}$"
    DATE_DATETIME = r"^[0-9]{4}((-)[0-9]{2}){2}([\s]{1}[0-9]{2}((:)[0-9]{2}){2})?$"
    AZURE_DWH_KEY_VAULT_URI = r"^(https:\/\/kv-ad-)((d)|(u)|(p))(-westeu-01\.vault\.azure\.net\/)$"
    AZURE_DWH_KEY_VAULT_SECRET_NAME = r"^(sec)((-)[a-z0-9]+)+$"
    UUID = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    UNICORN_API_ENTITY_SCHEMA_NAME = r"^[a-z_]+$"
    UNICORN_API_ENTITY_NAME = r"^([A-Z]+[a-z]*)+$"
    UNICORN_API_ENTITY_ATTRIBUTE_NAME = r"^([A-Z]{1}[a-z]+)+$"


# =========================================
# Key Vault helper
# =========================================
class AzureDWHKeyVaultHelper:
    _key_vault_uri = None

    @Decorator.validate_kwargs(schema={
        "type": "object",
        "properties": {
            "key_vault_uri": {
                "type": "string",
                "pattern": RegularExpression.AZURE_DWH_KEY_VAULT_URI
            }
        },
        "required": ["key_vault_uri"],
        "additionalProperties": False
    })
    def __init__(self, key_vault_uri: str) -> None:
        self._key_vault_uri = key_vault_uri

    @Decorator.validate_kwargs(schema={
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "pattern": RegularExpression.AZURE_DWH_KEY_VAULT_SECRET_NAME
            }
        },
        "required": ["name"],
        "additionalProperties": False
    })
    def get_secret_by_name(self, name: str) -> str:
        return getSecret(self._key_vault_uri, name)

class MicrosoftFabricLakehouseHelper:
    @classmethod
    @Decorator.validate_kwargs(schema={
        "type": "object",
        "properties": {
            "workspace_id": {
                "type": "string",
                "pattern": RegularExpression.UUID
            },
            "lakehouse_id": {
                "type": "string",
                "pattern": RegularExpression.UUID
            }
        },
        "required": ["workspace_id", "lakehouse_id"],
        "additionalProperties": False
    })
    def get_lakehouse_abfs_path(cls, workspace_id: str, lakehouse_id: str) -> str:
        return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/"

    @classmethod
    @Decorator.validate_kwargs(schema={
        "type": "object",
        "properties": {
            "workspace_id": {
                "type": "string",
                "pattern": RegularExpression.UUID
            },
            "lakehouse_id": {
                "type": "string",
                "pattern": RegularExpression.UUID
            },
            "entity_schema": {
                "type": "string",
                "pattern": RegularExpression.GENERAL_STRING
            },
            "entity": {
                "type": "string",
                "pattern": RegularExpression.GENERAL_STRING
            }
        },
        "required": ["workspace_id", "lakehouse_id", "entity_schema", "entity"],
        "additionalProperties": False
    })
    def get_delta_table_abfs_path(
        cls,
        workspace_id: str,
        lakehouse_id: str,
        entity_schema: str,
        entity: str
    ) -> str:
        return (
            f"{cls.get_lakehouse_abfs_path(workspace_id=workspace_id, lakehouse_id=lakehouse_id)}"
            f"Tables/{entity_schema}/{entity}"
        )

    @classmethod
    def get_delta_table_data_types(cls, delta_table: DeltaTable) -> dict:
        return {
            field.name: field.dataType.__class__.__name__
            for field in delta_table.toDF().schema
        }




# In[4]:


def get_tbl_path(bronze_layer: str, entity_schema: str, route: str) -> str:
    workspace_id = notebookutils.runtime.context["currentWorkspaceId"]

    lakehouse_id = list(
        filter(
            lambda lakehouse_info: lakehouse_info["displayName"] == bronze_layer,
            notebookutils.lakehouse.list(workspace_id)
        )
    )[0]["id"]

    return MicrosoftFabricLakehouseHelper.get_delta_table_abfs_path(
        workspace_id=workspace_id,
        lakehouse_id=lakehouse_id,
        entity_schema=entity_schema,
        entity=route
    )


def get_token(host: str, login: str, password: str) -> str:
    url = f"{host}getToken?login={login}&password={password}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()

    try:
        return r.json()["token"]
    except Exception:
        return r.text.strip()


class TokenManager:
    """Manages API token lifecycle with proactive TTL-based refresh and on-demand force refresh."""

    def __init__(self, host: str, login: str, password: str, ttl_seconds: int = 3300):
        self._host = host
        self._login = login
        self._password = password
        self._ttl = ttl_seconds  # default: 55 min (token expires at 60 min)
        self._token = None
        self._acquired_at: float = 0.0

    def get_token(self, force_refresh: bool = False) -> str:
        now = datetime.now(timezone.utc).timestamp()
        if force_refresh or self._token is None or (now - self._acquired_at) >= self._ttl:
            print("Acquiring/refreshing API token...")
            self._token = get_token(self._host, self._login, self._password)
            self._acquired_at = now
        return self._token


def request_with_retry(url: str, retries: int = 3, delay: int = 2) -> Any:
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=60)
            if r.status_code == 200:
                return r.json()
            raise Exception(f"HTTP {r.status_code}: {r.text[:500]}")
        except Exception:
            if attempt == retries - 1:
                raise
            sleep(delay * (attempt + 1))


def fetch_route(api_host, route, token_mgr: TokenManager, dtStart=None, dtEnd=None):
    page = 1
    all_rows = []

    while True:
        token = token_mgr.get_token()

        if dtStart and dtEnd:
            url = (
                f"{api_host}api/reports/{route}"
                f"?dtStart={dtStart}&dtEnd={dtEnd}"
                f"&page={page}&token={token}"
            )
        else:
            url = f"{api_host}api/reports/{route}?page={page}&token={token}"

        print(f"url: {url}")
        try:
            response = request_with_retry(url)
        except Exception as e:
            if "HTTP 401" in str(e) or "HTTP 403" in str(e):
                print(f"Token rejected on page {page} (route={route}), forcing refresh and retrying...")
                token = token_mgr.get_token(force_refresh=True)
                if dtStart and dtEnd:
                    url = (
                        f"{api_host}api/reports/{route}"
                        f"?dtStart={dtStart}&dtEnd={dtEnd}"
                        f"&page={page}&token={token}"
                    )
                else:
                    url = f"{api_host}api/reports/{route}?page={page}&token={token}"
                response = request_with_retry(url)
            else:
                raise

        if not isinstance(response, dict):
            raise ValueError(f"Unexpected response format for route={route}: {type(response)}")

        rows = response.get("data", [])
        current_page = response.get("currentPage")
        total_pages = response.get("totalPages")
        print(f"current page is {current_page}, total pages: {total_pages}")

        if rows:
            all_rows.extend(rows)

        if current_page is None or total_pages is None:
            raise ValueError(f"Pagination fields are missing for route={route}")

        if current_page >= total_pages:
            break

        page += 1

    print(f"Finished loading route={route}, rows={len(all_rows)}")
    return all_rows

def trg_tbl_is_empty(route: str, bronze_layer: str, entity_schema: str) -> bool:
    trg_table = get_tbl_path(
        bronze_layer=bronze_layer,
        entity_schema=entity_schema,
        route=route
    )

    df = spark.read.format("delta").load(trg_table)
    return df.isEmpty()


def add_tech_columns(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("TechExecutorRunID", F.lit(executor_run_id).cast(StringType()))
        .withColumn("TechProcessorRunID", F.lit(notebookutils.runtime.context["activityId"]).cast(StringType()))
        .withColumn(
            "TechProcessingDateTime",
            F.lit(processing_datetime.strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType())
        )
        .withColumn(
            "TechBusinessDateTime",
            F.lit(business_datetime.strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType())
        )
    )


def align_to_target_schema(src_df: DataFrame, target_path: str, route: str) -> DataFrame:
    trg_df = spark.read.format("delta").load(target_path)
    trg_schema = trg_df.schema

    target_cols = [f.name for f in trg_schema.fields]
    source_cols = src_df.columns

    target_set = set(target_cols)
    source_set = set(source_cols)

    extra_in_source = source_set - target_set
    missing_in_source = target_set - source_set

    if extra_in_source or missing_in_source:
        raise ValueError(
            f"Schema mismatch for route '{route}'. "
            f"Extra columns in source: {sorted(extra_in_source)}. "
            f"Missing columns in source: {sorted(missing_in_source)}"
        )

    return src_df.select(
        *[F.col(field.name).cast(field.dataType).alias(field.name) for field in trg_schema.fields]
    )


def fetch_reports(
    host: str,
    login: str,
    password: str,
    routes: Union[str, list],
    entity_schema: str,
    dtStart: str = None,
    dtEnd: str = None
) -> dict:
    if isinstance(routes, str):
        routes = [routes]

    token_mgr = TokenManager(host=host, login=login, password=password)
    results = {}

    for route in routes:
        print(f"Processing route: {route}")

        if route in FACT_TBLS:
            if trg_tbl_is_empty(
                route=route,
                bronze_layer="lhbronzead",
                entity_schema=entity_schema
            ):
                print("Target table is empty")
                current_date = datetime.now().strftime("%Y-%m-%d")
                rows = fetch_route(
                    api_host=host,
                    route=route,
                    token_mgr=token_mgr,
                    dtStart="2020-01-01",
                    dtEnd=current_date
                )
            else:
                rows = fetch_route(
                    api_host=host,
                    route=route,
                    token_mgr=token_mgr,
                    dtStart=dtStart,
                    dtEnd=dtEnd
                )
        else:
            rows = fetch_route(api_host=host, route=route, token_mgr=token_mgr)

        # Intentionally no empty dataframe handling.
        # If rows is empty and Spark cannot infer schema, notebook should fail.
        dfp = pd.DataFrame(rows)
        dfs = spark.createDataFrame(dfp)

        dfs = add_tech_columns(dfs)

        tablepath_stg = get_tbl_path(
            bronze_layer="lhbronzestgad",
            entity_schema=entity_schema,
            route=route
        )

        dfs.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(tablepath_stg)

        df_stg = spark.read.format("delta").load(tablepath_stg)

        tablepath = get_tbl_path(
            bronze_layer="lhbronzead",
            entity_schema=entity_schema,
            route=route
        )

        df_aligned = align_to_target_schema(
            src_df=df_stg,
            target_path=tablepath,
            route=route
        )

        df_aligned.write \
            .format("delta") \
            .mode("overwrite") \
            .option("partitionOverwriteMode", "dynamic") \
            .save(tablepath)

        results[route] = {
            "rows_fetched": len(rows),
            "stg_path": tablepath_stg,
            "target_path": tablepath
        }

    return results


# In[5]:


kv_helper = AzureDWHKeyVaultHelper(key_vault_uri=key_vault_uri)

api_host = kv_helper.get_secret_by_name(name = "sec-business-analytica-api-host")
api_login = kv_helper.get_secret_by_name(name = "sec-business-analytica-api-user-name")
api_password = kv_helper.get_secret_by_name(name = "sec-business-analytica-api-user-pass")

params_dict = json.loads(params)
print(f"Params dictionary: {params_dict}")

stats = fetch_reports(
    host = api_host,
    login=api_login,
    password=api_password,
    routes=params_dict["route"],
    entity_schema = params_dict["entity_schema"],
    dtStart=params_dict.get("dt_start", None), 
    dtEnd=params_dict.get("dt_end", None)
)
print(json.dumps(stats, indent=4, ensure_ascii=False))


# In[ ]:




