"""
dm_dynamodb.py

Wrapper para conexão DynamoDB adaptável por ambiente.
Substitui ``dm_redis.py`` — todas as operações usam uma tabela única
(single-table design) com PK/SK overloaded.

Comportamento:
  DEV  → boto3 com credenciais locais (~/.aws/credentials)
  PROD → boto3 com IAM task role (ECS Fargate injeta credenciais automaticamente)

Variáveis de ambiente:
  DYNAMODB_TABLE    : nome da tabela (padrão "dm-chain-explorer")
  AWS_DEFAULT_REGION: região AWS (padrão "sa-east-1")

Uso:
    from dm_chain_utils.dm_dynamodb import DMDynamoDB

    db = DMDynamoDB(logger=LOGGER)
    db.put_item("SEMAPHORE", "infura-api-key-1", {"process": "job-abc", "last_update": "..."})
    item = db.get_item("SEMAPHORE", "infura-api-key-1")
    items = db.query("SEMAPHORE")
    db.delete_item("SEMAPHORE", "infura-api-key-1")
"""

import logging
import os
import time
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


class DMDynamoDB:
    """
    Cliente DynamoDB reutilizável — single-table design.

    Todas as operações recebem ``pk`` e ``sk`` como strings.
    Atributos adicionais são passados como ``dict``.
    """

    def __init__(
        self,
        table_name: str | None = None,
        region: str | None = None,
        logger: logging.Logger | None = None,
    ):
        self.logger = logger or logging.getLogger(__name__)
        self.table_name = table_name or os.getenv("DYNAMODB_TABLE", "dm-chain-explorer")
        region = region or os.getenv("AWS_DEFAULT_REGION", "sa-east-1")

        self._resource = boto3.resource("dynamodb", region_name=region)
        self._table = self._resource.Table(self.table_name)

        self.logger.info(f"DMDynamoDB | table={self.table_name} | region={region}")

    # ------------------------------------------------------------------
    # Single-item operations
    # ------------------------------------------------------------------

    def put_item(
        self,
        pk: str,
        sk: str,
        attrs: dict[str, Any] | None = None,
        ttl_seconds: int | None = None,
    ) -> None:
        """
        Insere ou substitui um item.

        Parameters
        ----------
        pk          : partition key value (entity type, e.g. "SEMAPHORE")
        sk          : sort key value (entity id, e.g. "infura-api-key-1")
        attrs       : atributos adicionais (ex: {"process": "job-abc"})
        ttl_seconds : se fornecido, define atributo ``ttl`` com epoch + ttl_seconds
        """
        item: dict[str, Any] = {"pk": pk, "sk": sk}
        if attrs:
            item.update(self._convert_floats(attrs))
        if ttl_seconds is not None:
            item["ttl"] = int(time.time()) + ttl_seconds
        self._table.put_item(Item=item)

    def get_item(self, pk: str, sk: str) -> dict[str, Any] | None:
        """Retorna o item ou ``None`` se não existir."""
        resp = self._table.get_item(Key={"pk": pk, "sk": sk})
        item = resp.get("Item")
        if item:
            return self._convert_decimals(item)
        return None

    def delete_item(self, pk: str, sk: str) -> None:
        """Remove um item."""
        self._table.delete_item(Key={"pk": pk, "sk": sk})

    def update_item(
        self,
        pk: str,
        sk: str,
        updates: dict[str, Any],
        ttl_seconds: int | None = None,
    ) -> dict[str, Any]:
        """
        Atualiza atributos de um item existente (ou cria se não existir).

        Returns the updated attributes.
        """
        expr_parts = []
        expr_names = {}
        expr_values = {}

        for i, (attr_name, attr_value) in enumerate(updates.items()):
            placeholder_name = f"#a{i}"
            placeholder_val = f":v{i}"
            expr_parts.append(f"{placeholder_name} = {placeholder_val}")
            expr_names[placeholder_name] = attr_name
            expr_values[placeholder_val] = self._convert_float_value(attr_value)

        if ttl_seconds is not None:
            expr_parts.append("#ttl = :ttl_val")
            expr_names["#ttl"] = "ttl"
            expr_values[":ttl_val"] = int(time.time()) + ttl_seconds

        update_expr = "SET " + ", ".join(expr_parts)

        resp = self._table.update_item(
            Key={"pk": pk, "sk": sk},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_values,
            ReturnValues="ALL_NEW",
        )
        return self._convert_decimals(resp.get("Attributes", {}))

    def conditional_put_item(
        self,
        pk: str,
        sk: str,
        attrs: dict[str, Any] | None = None,
        ttl_seconds: int | None = None,
        condition_expression: str | None = None,
    ) -> bool:
        """
        PutItem condicional — retorna True se bem-sucedido, False se a condição falhou.

        Usado para implementar locks otimistas (ex: semáforo de API keys).
        """
        item: dict[str, Any] = {"pk": pk, "sk": sk}
        if attrs:
            item.update(self._convert_floats(attrs))
        if ttl_seconds is not None:
            item["ttl"] = int(time.time()) + ttl_seconds

        kwargs: dict[str, Any] = {"Item": item}
        if condition_expression:
            kwargs["ConditionExpression"] = condition_expression

        try:
            self._table.put_item(**kwargs)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    # ------------------------------------------------------------------
    # Query operations
    # ------------------------------------------------------------------

    def query(
        self,
        pk: str,
        sk_prefix: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Query por partition key, opcionalmente filtrando por prefixo de sort key.

        Retorna lista de dicts (itens).
        """
        key_condition = Key("pk").eq(pk)
        if sk_prefix:
            key_condition = key_condition & Key("sk").begins_with(sk_prefix)

        kwargs: dict[str, Any] = {"KeyConditionExpression": key_condition}
        if limit:
            kwargs["Limit"] = limit

        items = []
        while True:
            resp = self._table.query(**kwargs)
            items.extend(resp.get("Items", []))
            last_key = resp.get("LastEvaluatedKey")
            if not last_key or (limit and len(items) >= limit):
                break
            kwargs["ExclusiveStartKey"] = last_key

        return [self._convert_decimals(item) for item in items]

    def query_all_keys(self, pk: str) -> list[str]:
        """Retorna apenas os sort keys para uma dada partition key."""
        items = self.query(pk)
        return [item["sk"] for item in items]

    # ------------------------------------------------------------------
    # Batch operations
    # ------------------------------------------------------------------

    def batch_write(self, items: list[dict[str, Any]]) -> None:
        """
        Batch write de múltiplos itens (máx 25 por batch, auto-paginado).

        Cada item deve conter "pk" e "sk" no mínimo.
        """
        with self._table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=self._convert_floats(item))

    def batch_delete(self, keys: list[dict[str, str]]) -> None:
        """
        Batch delete de múltiplos itens.

        keys: lista de {"pk": ..., "sk": ...}
        """
        with self._table.batch_writer() as batch:
            for key in keys:
                batch.delete_item(Key={"pk": key["pk"], "sk": key["sk"]})

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def item_exists(self, pk: str, sk: str) -> bool:
        """Verifica se um item existe (sem retornar o conteúdo)."""
        resp = self._table.get_item(
            Key={"pk": pk, "sk": sk},
            ProjectionExpression="pk",
        )
        return "Item" in resp

    def delete_all_by_pk(self, pk: str) -> int:
        """Remove todos os itens de uma partition key. Retorna quantidade deletada."""
        items = self.query(pk)
        keys = [{"pk": item["pk"], "sk": item["sk"]} for item in items]
        if keys:
            self.batch_delete(keys)
        return len(keys)

    def ping(self) -> bool:
        """Healthcheck — verifica se a tabela existe e está acessível."""
        try:
            _ = self._table.table_status
            return True
        except ClientError:
            return False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _convert_floats(data: dict[str, Any]) -> dict[str, Any]:
        """Converte float → Decimal (DynamoDB não aceita float)."""
        result = {}
        for k, v in data.items():
            result[k] = DMDynamoDB._convert_float_value(v)
        return result

    @staticmethod
    def _convert_float_value(value: Any) -> Any:
        """Converte um valor float individual para Decimal."""
        if isinstance(value, float):
            return Decimal(str(value))
        if isinstance(value, dict):
            return DMDynamoDB._convert_floats(value)
        if isinstance(value, list):
            return [DMDynamoDB._convert_float_value(v) for v in value]
        return value

    @staticmethod
    def _convert_decimals(data: dict[str, Any]) -> dict[str, Any]:
        """Converte Decimal → int/float em respostas do DynamoDB."""
        result: dict[str, Any] = {}
        for k, v in data.items():
            if isinstance(v, Decimal):
                result[k] = int(v) if v == int(v) else float(v)
            elif isinstance(v, dict):
                result[k] = DMDynamoDB._convert_decimals(v)
            elif isinstance(v, list):
                result[k] = [
                    DMDynamoDB._convert_decimals(i)
                    if isinstance(i, dict)
                    else (
                        int(i)
                        if isinstance(i, Decimal) and i == int(i)
                        else float(i)
                        if isinstance(i, Decimal)
                        else i
                    )
                    for i in v
                ]
            else:
                result[k] = v
        return result
