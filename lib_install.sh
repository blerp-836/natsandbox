python setup.py bdist_wheel
#databricks fs mkdirs dbfs:/libraries
databricks fs cp --overwrite /home/natmsdnadmin/develop/sandbox/dist/sandbox-0.0.2-py3-none-any.whl dbfs:/libraries/
databricks libraries uninstall --cluster-id 0321-005755-4jemos23 --whl dbfs:/libraries/sandbox-0.0.2-py3-none-any.whl
databricks clusters restart --cluster-id 0321-005755-4jemos23
####################
databricks libraries install --cluster-id 0321-005755-4jemos23 --pypi-package azure-common==1.1.27
databricks libraries install --cluster-id 0321-005755-4jemos23 --pypi-package azure-eventhub==5.5.0
databricks libraries install --cluster-id 0321-005755-4jemos23 --pypi-package azure-common==1.1.27
databricks libraries install --cluster-id 0321-005755-4jemos23 --pypi-package azure-eventhub==5.5.0
databricks libraries install --cluster-id 0321-005755-4jemos23 --pypi-package azure-core==1.14.0
databricks libraries install --cluster-id 0321-005755-4jemos23 --pypi-package azure-identity==1.5.0
databricks libraries install --cluster-id 0321-005755-4jemos23 --pypi-package azure-keyvault-secrets==4.2.0
databricks libraries install --cluster-id 0321-005755-4jemos23 --pypi-package azure-storage-blob==12.8.1
databricks libraries install --cluster-id 0321-005755-4jemos23 --pypi-package azure-storage-common==2.1.0
databricks libraries install --cluster-id 0321-005755-4jemos23 --pypi-package azure-storage-file-datalake==12.3.1
databricks libraries install --cluster-id 0321-005755-4jemos23 --maven-coordinates com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.15
databricks libraries install --cluster-id 0321-005755-4jemos23 --pypi-package xmlrunner
databricks fs mkdirs dbfs:/libraries
databricks libraries install --cluster-id 0321-005755-4jemos23 --whl dbfs:/libraries/sandbox-0.0.2-py3-none-any.whl
databricks libraries list --cluster-id 0321-005755-4jemos23


python setup.py bdist_wheel
databricks fs mkdirs dbfs:/libraries
databricks fs cp --overwrite /home/natmsdnadmin/develop/sandbox/dist/sandbox-0.0.2-py3-none-any.whl dbfs:/libraries/
databricks libraries uninstall --cluster-id 0321-005755-4jemos23 --whl dbfs:/libraries/sandbox-0.0.2-py3-none-any.whl
databricks clusters restart --cluster-id 0321-005755-4jemos23
databricks libraries install --cluster-id 0321-005755-4jemos23 --whl dbfs:/libraries/sandbox-0.0.2-py3-none-any.whl
python /home/natmsdnadmin/develop/sandbox/utilities/BlobStorage.py
python /home/natmsdnadmin/develop/sandbox/utilities/BlobStorage.py
databricks workspace import --overwrite /home/natmsdnadmin/develop/sandbox/adls/notebook_workflow.py /Users/natalia.theodora@accenture.com/adls/notebook_workflow -l python
databricks workspace import --overwrite /home/natmsdnadmin/develop/sandbox/adls/delta_workflow.py /Users/natalia.theodora@accenture.com/adls/delta_workflow -l python
databricks workspace import --overwrite /home/natmsdnadmin/develop/sandbox/adls/video_workflow.py /Users/natalia.theodora@accenture.com/adls/video_workflow -l python
databricks workspace import --overwrite /home/natmsdnadmin/develop/sandbox/adls/videoCat_workflow.py /Users/natalia.theodora@accenture.com/adls/videoCat_workflow -l python
databricks workspace import --overwrite /home/natmsdnadmin/develop/sandbox/adls/regions_wf.py /Users/natalia.theodora@accenture.com/adls/regions_wf -l python