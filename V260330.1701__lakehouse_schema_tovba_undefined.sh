#!/bin/sh
set -eu

printenv

echo "[lh-provisioner] Initialization..."

PY_PROVISIONER_PATH="/flyway/lh-provisioner/lh_provisioner.py"

FABRIC_WORKSPACE_ID=${FP__fabric_workspace_id__}
FABRIC_BRONZE_LAKEHOUSE_ID=${FP__fabric_bronze_lakehouse_id__}

echo "[lh-provisioner] Fabric workspace name: ${FABRIC_WORKSPACE_ID}"
echo "[lh-provisioner] Fabric bronze lakehouse name: ${FABRIC_BRONZE_LAKEHOUSE_ID}"

# Create a restricted temp YAML file
umask 077
CFG_FILE="$(mktemp -t onelake_tables.XXXXXX.yaml)"

echo "[lh-provisioner] Temp file created: ${CFG_FILE}"

cleanup() {
  rm -f "${CFG_FILE}"
}
trap cleanup EXIT

# ---- INLINE YAML CONFIG (edit to your needs) ----
echo "[lh-provisioner] Writing YAML content to: ${CFG_FILE}"
cat > "$CFG_FILE" <<YAML
lakehouse_base_uri: "abfss://${FABRIC_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/${FABRIC_BRONZE_LAKEHOUSE_ID}"
tables_folder: "Tables"

tables:
- name: sales
  schema_name: tovba_undefined
  columns:
    - { name: TechExecutorRunID, type: string, nullable: true }
    - { name: TechProcessorRunID, type: string, nullable: true }
    - { name: TechProcessingDateTime, type: timestamp, nullable: true }
    - { name: TechBusinessDateTime, type: timestamp, nullable: true }
    - { name: day, type: string, nullable: false }
    - { name: buyerid, type: int, nullable: true }
    - { name: outletref, type: string, nullable: true }
    - { name: docref, type: string, nullable: true }
    - { name: productid, type: int, nullable: true }
    - { name: doctypeid, type: int, nullable: true }
    - { name: empref, type: string, nullable: true }
    - { name: qty, type: int, nullable: true }
    - { name: sum, type: "decimal(38,3)", nullable: true }
  partition_by: ['day']
- name: stocks
  schema_name: tovba_undefined
  columns:
    - { name: TechExecutorRunID, type: string, nullable: true }
    - { name: TechProcessorRunID, type: string, nullable: true }
    - { name: TechProcessingDateTime, type: timestamp, nullable: true }
    - { name: TechBusinessDateTime, type: timestamp, nullable: true }
    - { name: day, type: string, nullable: false }
    - { name: buyerid, type: int, nullable: true }
    - { name: productid, type: int, nullable: true }
    - { name: qty, type: int, nullable: true }
    - { name: sum, type: "decimal(38,3)", nullable: true }
    - { name: liters, type: "decimal(38,2)", nullable: true }
    - { name: promoid, type: int, nullable: true }
  partition_by: ['day']
- name: docs
  schema_name: tovba_undefined
  columns:
    - { name: TechExecutorRunID, type: string, nullable: true }
    - { name: TechProcessorRunID, type: string, nullable: true }
    - { name: TechProcessingDateTime, type: timestamp, nullable: true }
    - { name: TechBusinessDateTime, type: timestamp, nullable: true }
    - { name: docref, type: string, nullable: false }
    - { name: buyerid, type: int, nullable: true }
    - { name: day, type: string, nullable: false }
    - { name: docno, type: string, nullable: true }
    - { name: doctypeid, type: int, nullable: true }
    - { name: doctypename, type: string, nullable: true }
  partition_by: ['day']
- name: emp
  schema_name: tovba_undefined
  columns:
    - { name: TechExecutorRunID, type: string, nullable: true }
    - { name: TechProcessorRunID, type: string, nullable: true }
    - { name: TechProcessingDateTime, type: timestamp, nullable: true }
    - { name: TechBusinessDateTime, type: timestamp, nullable: true }
    - { name: empref, type: string, nullable: false }
    - { name: buyerid, type: int, nullable: true }
    - { name: salerepname, type: string, nullable: false }
    - { name: supervisorname, type: string, nullable: true }
    - { name: salereptype, type: string, nullable: true }
- name: buyers
  schema_name: tovba_undefined
  columns:
    - { name: TechExecutorRunID, type: string, nullable: true }
    - { name: TechProcessorRunID, type: string, nullable: true }
    - { name: TechProcessingDateTime, type: timestamp, nullable: true }
    - { name: TechBusinessDateTime, type: timestamp, nullable: true }
    - { name: buyerid, type: int, nullable: false }
    - { name: buyername, type: string, nullable: true }
    - { name: buyerstoragename, type: string, nullable: true }
    - { name: activity, type: string, nullable: true }
    - { name: country, type: string, nullable: true }
    - { name: closingdate, type: string, nullable: true }
    - { name: curname, type: string, nullable: true }
    - { name: curnumber, type: string, nullable: true }
    - { name: pricecurnumber, type: string, nullable: true }
    - { name: tmid, type: int, nullable: true }
    - { name: dmid, type: int, nullable: true }
    - { name: svid, type: int, nullable: true }
    - { name: geoid, type: string, nullable: true }
    - { name: channel, type: string, nullable: true }
    - { name: buyerregion, type: string, nullable: true }
    - { name: balance, type: string, nullable: true }
    - { name: representation, type: string, nullable: true }
    - { name: tm, type: string, nullable: true }
    - { name: dm, type: string, nullable: true }
    - { name: sv, type: string, nullable: true }
    - { name: region, type: string, nullable: true }
    - { name: city, type: string, nullable: true }
- name: outlets
  schema_name: tovba_undefined
  columns:
    - { name: TechExecutorRunID, type: string, nullable: true }
    - { name: TechProcessorRunID, type: string, nullable: true }
    - { name: TechProcessingDateTime, type: timestamp, nullable: true }
    - { name: TechBusinessDateTime, type: timestamp, nullable: true }
    - { name: outletref, type: string, nullable: false }
    - { name: outletuniqueref, type: string, nullable: true }
    - { name: buyerid, type: int, nullable: true }
    - { name: outletname, type: string, nullable: true }
    - { name: outletaddress, type: string, nullable: true }
    - { name: clientname, type: string, nullable: true }
    - { name: custchain, type: string, nullable: true }
    - { name: categoryid, type: int, nullable: true }
    - { name: edrpou, type: string, nullable: true }
    - { name: region, type: string, nullable: true }
    - { name: area, type: string, nullable: true }
    - { name: city, type: string, nullable: true }
    - { name: categorytt, type: string, nullable: true }
    - { name: chainname, type: string, nullable: true }
- name: products
  schema_name: tovba_undefined
  columns:
    - { name: TechExecutorRunID, type: string, nullable: true }
    - { name: TechProcessorRunID, type: string, nullable: true }
    - { name: TechProcessingDateTime, type: timestamp, nullable: true }
    - { name: TechBusinessDateTime, type: timestamp, nullable: true }
    - { name: productid, type: int, nullable: false }
    - { name: erpcode, type: string, nullable: true }
    - { name: picture, type: string, nullable: true }
    - { name: productname, type: string, nullable: true }
    - { name: productnamefull, type: string, nullable: true }
    - { name: ean, type: string, nullable: true }
    - { name: category, type: string, nullable: true }
    - { name: subcategory, type: string, nullable: true }
    - { name: brand, type: string, nullable: true }
    - { name: weight, type: "decimal(38,2)", nullable: true }
    - { name: package, type: string, nullable: true }
    - { name: sortorder, type: "decimal(38,2)", nullable: true }
    - { name: units, type: string, nullable: true }
    - { name: vat, type: "decimal(38,1)", nullable: true }
    - { name: skuba, type: string, nullable: true }
    - { name: shelflife, type: string, nullable: true }
    - { name: productgroup, type: string, nullable: true }
    - { name: productsubgroup, type: string, nullable: true }
    - { name: qtybox, type: int, nullable: true }
    - { name: qtypack, type: int, nullable: true }
    - { name: qtyset, type: int, nullable: true }
    - { name: brandid, type: int, nullable: true }
    - { name: volume, type: int, nullable: true }
    - { name: activity, type: string, nullable: true }
    - { name: country, type: string, nullable: true }
    - { name: color, type: string, nullable: true }
    - { name: brandname, type: string, nullable: true }
YAML

echo "[lh-provisioner] YAML config initialized: ${CFG_FILE}"
echo "[lh-provisioner] YAML content: "
cat "${CFG_FILE}"
# -----------------------------------------------

echo "[lh-provisioner] Using config: ${CFG_FILE}"

# Run the helper (fails the script on error)
#AZ_TENANT_ID="${AZ_TENANT_ID}" \
#AZ_CLIENT_ID="${AZ_CLIENT_ID}" \
#AZ_CLIENT_SECRET="${AZ_CLIENT_SECRET}" \
python3 "${PY_PROVISIONER_PATH}" --evolve-schema -c "${CFG_FILE}"

echo "[lh-provisioner] Done."