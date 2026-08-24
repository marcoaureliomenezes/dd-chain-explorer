"""
AWS SSM Parameter Store client.

Uso:
    from dm_chain_utils.dm_parameter_store import ParameterStoreClient

    client = ParameterStoreClient()
    api_key = client.get_parameter("/web3-api-keys/alchemy/api-key-1")
"""

import logging

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger(__name__)


class ParameterStoreClient:
    def __init__(self, region_name: str = "sa-east-1"):
        """
        Parameters
        ----------
        region_name : str
            Região AWS onde os parâmetros estão armazenados.
        """
        self._client = boto3.client("ssm", region_name=region_name)
        logger.info(f"[ParameterStore] Client initialized (region='{region_name}')")

    def get_parameter(self, name: str) -> str | None:
        """
        Busca um parâmetro SecureString no Parameter Store.

        Returns
        -------
        str | None
            Valor do parâmetro, ou None em caso de erro.
        """
        try:
            response = self._client.get_parameter(Name=name, WithDecryption=True)
            value: str | None = response.get("Parameter", {}).get("Value")
            logger.info(f"[ParameterStore] Retrieved: {name}")
            return value
        except ClientError as e:
            logger.error(f"[ParameterStore] ClientError fetching '{name}': {e}")
            return None
        except NoCredentialsError as e:
            logger.error(f"[ParameterStore] No AWS credentials available: {e}")
            return None

    def put_parameter(
        self,
        name: str,
        value: str,
        description: str = "",
        overwrite: bool = False,
    ) -> bool:
        """
        Cria ou atualiza um parâmetro SecureString no Parameter Store.

        Returns
        -------
        bool
            True em caso de sucesso, False em caso de erro.
        """
        try:
            kwargs = dict(
                Name=name,
                Value=value,
                Type="SecureString",
                Overwrite=overwrite,
            )
            if description:
                kwargs["Description"] = description
            self._client.put_parameter(**kwargs)
            logger.info(f"[ParameterStore] Created: {name}")
            return True
        except ClientError as e:
            logger.error(f"[ParameterStore] ClientError creating '{name}': {e}")
            return False
        except NoCredentialsError as e:
            logger.error(f"[ParameterStore] No AWS credentials available: {e}")
            return False

    def delete_parameter(self, name: str) -> bool:
        """Remove um parâmetro do Parameter Store."""
        try:
            self._client.delete_parameter(Name=name)
            logger.info(f"[ParameterStore] Deleted: {name}")
            return True
        except ClientError as e:
            logger.error(f"[ParameterStore] ClientError deleting '{name}': {e}")
            return False
        except NoCredentialsError as e:
            logger.error(f"[ParameterStore] No AWS credentials: {e}")
            return False

    def get_parameters_by_path(self, path: str) -> dict[str, str]:
        """
        Lista todos os parâmetros sob um prefixo hierárquico.

        Parameters
        ----------
        path : str
            Prefixo hierárquico. Ex: '/web3-api-keys/infura/'

        Returns
        -------
        dict[str, str]
            Mapeamento { nome_completo: valor }.
        """
        params: dict[str, str] = {}
        paginator = self._client.get_paginator("get_parameters_by_path")
        try:
            for page in paginator.paginate(Path=path, WithDecryption=True, Recursive=True):
                for param in page.get("Parameters", []):
                    params[param["Name"]] = param["Value"]
        except (ClientError, NoCredentialsError) as e:
            logger.error(f"[ParameterStore] Error fetching path '{path}': {e}")
        logger.info(f"[ParameterStore] {len(params)} params under '{path}'.")
        return params
