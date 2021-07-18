python setup.py bdist_wheel
databricks fs mkdirs dbfs:/libraries
databricks fs cp --overwrite /home/natmsdnadmin/develop/sandbox/dist/sandbox-0.0.2-py3-none-any.whl dbfs:/libraries/
databricks libraries uninstall --cluster-id <<cluster_id>> --whl dbfs:/libraries/sandbox-0.0.2-py3-none-any.whl
databricks clusters restart --cluster-id <<cluster_id>>
####################
databricks libraries install --cluster-id <<cluster_id>> --pypi-package azure-common==1.1.27
databricks libraries install --cluster-id <<cluster_id>> --pypi-package azure-eventhub==5.5.0
databricks libraries install --cluster-id <<cluster_id>> --pypi-package azure-common==1.1.27
databricks libraries install --cluster-id <<cluster_id>> --pypi-package azure-eventhub==5.5.0
databricks libraries install --cluster-id <<cluster_id>> --pypi-package azure-core==1.14.0
databricks libraries install --cluster-id <<cluster_id>> --pypi-package azure-identity==1.5.0
databricks libraries install --cluster-id <<cluster_id>> --pypi-package azure-keyvault-secrets==4.2.0
databricks libraries install --cluster-id <<cluster_id>> --pypi-package azure-storage-blob==12.8.1
databricks libraries install --cluster-id <<cluster_id>> --pypi-package azure-storage-common==2.1.0
databricks libraries install --cluster-id <<cluster_id>> --pypi-package azure-storage-file-datalake==12.3.1
databricks libraries install --cluster-id <<cluster_id>> --maven-coordinates com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.15
databricks libraries install --cluster-id <<cluster_id>> --whl dbfs:/libraries/sandbox-0.0.2-py3-none-any.whl
databricks libraries list --cluster-id <<cluster_id>>