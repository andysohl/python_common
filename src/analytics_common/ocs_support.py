# -*- coding: utf-8 -*-
import errno
import os

from . import CSV_IMPORT_OPTIONS

import re
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import BooleanType, DecimalType, IntegerType, StructType, StructField, DateType, StringType
from py4j.protocol import Py4JJavaError
import pyspark.sql.window as w

# Constants
OCS_SYSTEM_NAME = "OCS"
COMPANY_CODE = "TRI"

PLACE_MAPPING = {
    "AFR": "ZA", "AFR0101": "ZA", "AMS": "AS", "AMS0101": "AS", "ARG": "AR",
    "ARG0101": "AR", "AUR": "AT", "AUS": "AU", "AUS0101": "AU", "BAH": "BS",
    "BAH0101": "BS", "BBD": "BB", "BEL": "BE", "BEL0101": "BE", "BEM": "BM",
    "BEM0101": "BM", "BRA": "BR", "BRA0101": "BR", "BRN": "BN", "BUR": "MM",
    "BVI": "VG", "BVI0101": "VG", "BWI": None, "BWI0101": None, "CAN": "CA",
    "CAN0101": "CA", "CAY": "KY", "CAY0101": "KY", "CHN": "CN", "CHN0101": "CN",
    "CHN0102": "CN", "CHN0103": "CN", "CHN0200": "CN", "CHN0300": "CN", "CHN0500": "CN",
    "CHN0501": "CN", "CHN0502": "CN", "CHN0503": "CN", "CHN0504": "CN", "CHN0505": "CN",
    "CHN0506": "CN", "CHN0507": "CN", "CHN0512": "CN", "CHN0564": "CN", "CHN0570": "CN",
    "CHN0907": "CN", "CHN1414": "CN", "CHN1420": "CN", "CHN1704": "CN", "CHN1714": "CN",
    "CHN2209": "CN", "CHN2221": "CN", "CHN2300": "CN", "CHN2600": "CN", "CHN2700": "CN",
    "COO": "CK", "COO0101": "CK", "CUB": "CU", "CYP": "CY", "CZE": "CZ",
    "CZE0101": "CZ", "DEN": "DK", "DEN0101": "DK", "DEU": "DE", "DOR": None,
    "ENG": "GB", "ENG0101": "GB", "FIN": "FI", "FIN0101": "FI", "FRA": "FR",
    "FRA0101": "FR", "GER": "DE", "GER0101": "DE", "GRE": "GR", "HKG": "HK",
    "HKG0101": "HK", "HOL": "NL", "HOL0101": "NL", "IDA": "IN", "IDA0101": "IN",
    "IND": "ID", "IND0101": "ID", "IRA": "IR", "IRA0101": "IR", "IRE": "IE",
    "IRE0101": "IE", "ISM": "IM", "ISM0101": "IM", "ISR": "IL", "ISR0101": "IL",
    "ITA": "IT", "ITA0101": "IT", "JAP": "JP", "JAP0101": "JP", "JER": "JE",
    "JER0101": "JE", "KEN": "KE", "KEN0101": "KE", "KOR": "KR", "KOR0101": "KR",
    "LAB": "MY", "LAB0101": "MY", "LIB": "LR", "LIB0101": "LR", "LUX": "LU",
    "MAC": "MO", "MAC0101": "MO", "MAL": "MY", "MAL0101": "MY", "MYA": "MM",
    "NET": "NL", "NET0101": "NL", "NKO": "KP", "NOR": "NO", "NOR0101": "NO",
    "NSW0101": "AU", "NZE": "NZ", "NZE0101": "NZ", "PAK": "PK", "PAK0101": "PK",
    "PAN": "PA", "PAN0101": "PA", "PHL": "PH", "PHL0101": "PH", "POR": "US",
    "POR0101": "US", "RSA": "ZA", "RSA0101": "ZA", "SDN": "SD", "SDN0101": "SD",
    "SIN": "SG", "SIN0101": "SG", "SPA": "ES", "SPA0101": "ES", "SUD": "SD",
    "SWI": "CH", "SWI0101": "CH", "SYR": "SY", "THL": "TH", "THL0101": "TH",
    "TWN": "TW", "TWN0101": "TW", "UAE": "AE", "USA": "US", "USA0101": "US",
    "VIE": "VN", "VIE0101": "VN", "WAU": "AU", "WAU0101": "AU", "WSM": "WS",
    "WSM0101": "WS", "ZZZ": "None", "ZZZ7777": "None",
}

def load_table(base_path: str, bu: str, table: str, environment: str = "PROD", tz: str = "Asia/Hong_Kong") -> DataFrame:
    """Load an OCS table, applying any filtering/cleaning before returning a dataframe"""

    non_standard_tables = ["BankAccount", "ClientVirtualAccount"]

    s = SparkSession.getActiveSession()  # type: SparkSession
    log4jLogger = s._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    base_path = base_path.rstrip('\\')
    bu = bu.upper()

    table_ext = ""
    if table not in non_standard_tables:
        table_ext = "_TBL"
        table = table.upper()

    environment = environment.upper()
    exchange_rate_precision = DecimalType(20, 15)
    decimal_precision = DecimalType(10, 2)

    def _get_scala_object(name: str) -> object:
        clazz = s.sparkContext._jvm.org.apache.spark.util.Utils.classForName(name + "$", True, False)
        ff = clazz.getDeclaredField("MODULE$")
        return ff.get(None)

    def _create_multi_format_date_parser(col_name: str):
        """A multi-date parser for Excel"""
        return when(
            col(col_name).rlike(r'^\d{4}[-\\/]\d{2}[-\\/]\d{2}$'),
            date_trunc('day', to_timestamp(col(col_name), "yyyy/MM/dd"))
        ).when(
            col(col_name).rlike(r'^\d{1,2}[-\\/]\d{1,2}[-\\/]\d{2}$'),
            date_trunc('day', to_timestamp(col(col_name), "M/d/yy"))
        ).otherwise(lit(None))

    # Need to do this for OCS because of the format.
    s.conf.set("textinputformat.record.delimiter", "\r\n")
    s.sparkContext._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "\r\n")

    # Handle meta tables.
    if "CLIENT_RELATIONSHIPS" == table or "SERVICE_CODE_MAPPING" == table or "OFFICE_CODE_MAPPING" == table:
        full_path = "{base}/Bronze/{bu}/OCS/{environment}/{table}.xlsx".format(
            base=base_path, bu=bu, environment=environment, table=table)
        try:
            logger.info("Attempting to open meta table {table}".format(table=table))
            df = s.read.format("com.crealytics.spark.excel") \
                .option("dataAddress", table+"!A1").option("header", "true").load(full_path)

            # Normalise the column names
            name_normaliser = str.maketrans(" ", "_")
            df = df.select(
                [col(name).alias(name.lower().translate(name_normaliser)) for name in df.columns]
            )
        except Py4JJavaError as err:
            logger.info("Meta table not found ({err})".format(table=table, err=err.errmsg))
            df = None
    elif "STAFF_COST" == table:
        full_path = "{base}/Bronze/{bu}/OCS/{environment}/{table}.xlsx".format(
            base=base_path, bu=bu, environment=environment, table=table)
        try:
            logger.info("Attempting to open meta table {table}".format(table=table))

            # Slow and ugly, but no way to do this without importing lots of extra
            # The complexity is because of the Scala code.
            argmap = s.sparkContext._jvm.PythonUtils.toScalaMap({"path": full_path})
            hadoop_config = s.sparkContext._jsc.hadoopConfiguration()

            wb_obj = _get_scala_object("com.crealytics.spark.excel.WorkbookReader")
            sheetnames = wb_obj.apply(argmap, hadoop_config).sheetNames()

            year_pattern = re.compile("^\d{4}$")
            df = None
            for i in range(0, sheetnames.length()):
                sn = sheetnames.apply(i)
                if year_pattern.fullmatch(sn) is None:
                    continue
                logger.info("Loading cost data for " + sn)

                if df is None:
                    df = s.read.format("com.crealytics.spark.excel") \
                        .option("dataAddress", sn + "!A1").option("header", "true").load(full_path) \
                        .withColumn("year", lit(sn))
                else:
                    df = df.union(
                            s.read.format("com.crealytics.spark.excel")
                            .option("dataAddress", sn + "!A1").option("header", "true").load(full_path)
                            .withColumn("year", lit(sn))
                    )

            if df is None:
                logger.fatal("No data found in FTE cost file")

            # Normalise the column names
            name_normaliser = str.maketrans(" ", "_")
            df = df.select(
                [col(name).alias(name.lower().translate(name_normaliser)) for name in df.columns]
            )
        except Py4JJavaError as err:
            logger.info("Meta table not found or unable to load ({err})".format(table=table, err=err.errmsg))
            raise err
    else:
        csv_import_options = CSV_IMPORT_OPTIONS.copy()
        if "OCSCARFM" == table:
            csv_import_options["multiLine"] = "true"

        full_path = "{base}/Bronze/{bu}/OCS/{environment}/{table}{ext}".format(
            base=base_path, bu=bu, environment=environment, table=table, ext=table_ext)
        df = s.read.format("csv").options(**CSV_IMPORT_OPTIONS).load(full_path)

    # Apply any known, table specific cleaning
    # Note that we purposely create a new key here because many tables have a compound key which causes havoc in
    # power BI and means that people need to be aware of the key material on each table. This is done while fully
    # aware that the record may be re-keyed at a later time.
    # Keys will always be largest to smallest, i.e. BU/Country (i.e. many sources), source (i.e. many codes), code
    if "OCSMCLNT" == table:  # Clients
        df = df \
            .repartition(s._sc.defaultParallelism, col("CLIENT_CODE")) \
            .filter(col("CLIENT_CODE").isNotNull() & (col("CLIENT_CODE") != "") & (col("CLIENT_CODE") != "000000")) \
            .withColumn("BU", lit(bu)) \
            .withColumn("SOURCE", lit(OCS_SYSTEM_NAME)) \
            .withColumn("KEY", lpad(hex(xxhash64(col("BU"), col("SOURCE"), col("CLIENT_CODE"))), 16, "0")) \
            .withColumn("PARENT_KEY",
                        when(col("PARENT_GROUP").isNull() | (col("PARENT_GROUP") == col("CLIENT_CODE")), None)
                        .otherwise(lpad(hex(xxhash64(col("BU"), col("SOURCE"), col("PARENT_GROUP"))), 16, "0"))) \
            .withColumn("LEAD_PARTNER_KEY",  # Lead partner key
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("LEAD_PARTNER"))), 16, "0")) \
            .withColumn("CLIENT_NAME",
                        when((length(trim(col("CLIENT_NAME1"))) == 35)
                             & (~col("CLIENT_NAME2").rlike(r"(?i)^(?:CO\.?,?\s*(?:LTD|LIMITED)\.?)$")),
                             concat(col("CLIENT_NAME1"), col("CLIENT_NAME2")))
                        .otherwise(trim(concat_ws(" ", col("CLIENT_NAME1"), col("CLIENT_NAME2"))))) \
            .filter(col("CLIENT_NAME").isNotNull()
                    & (~col("CLIENT_NAME").isin("", "*", "OCS", "WEB BASED OCS"))
                    & (substring(col("CLIENT_CODE"), 0, 1) != "9")
                    & (instr(col("CLIENT_NAME"), "TRICOR") == 0)) \
            .withColumn("LISTING_DATE", to_date(to_timestamp(col("LISTING_DATE")))) \
            .withColumn("START_DATE", to_date(to_timestamp(col("START_DATE")))) \
            .withColumn("INPUT_DATE", to_timestamp(col("INPUT_DATE"))) \
            .withColumn("LAST_UPDATE_DATE", to_timestamp(col("LAST_UPDATE_DATE"))) \
            .withColumn("DELETE_DATE", to_timestamp(col("DELETE_DATE"))) \
            .withColumn("IS_SUBSIDIARY", col("IS_SUBSIDIARY").cast(BooleanType())) \
            .withColumn("IS_LISTED_COMP", col("IS_LISTED_COMP").cast(BooleanType())) \
            .withColumn("START_DATE", when(col("START_DATE") > date_add(current_date(), 365), lit(None))
                        .otherwise(col("START_DATE")))
    elif "OCSMCNGP" == table:  # Client Groups
        df = df \
            .repartition(s._sc.defaultParallelism, col("GROUP_CODE")) \
            .filter(col("GROUP_CODE").isNotNull() & (col("GROUP_CODE") != "") & (col("GROUP_CODE") != "000000")) \
            .withColumn("BU", lit(bu)) \
            .withColumn("SOURCE", lit(OCS_SYSTEM_NAME)) \
            .withColumn("KEY", lpad(hex(xxhash64(col("BU"), col("SOURCE"), col("GROUP_CODE"))), 16, "0")) \
            .withColumn("GROUP_NAME",
                        when(length(trim(col("GROUP_NAME1"))) == 35, concat(col("GROUP_NAME1"), col("GROUP_NAME2")))
                        .otherwise(trim(concat_ws(" ", col("GROUP_NAME1"), col("GROUP_NAME2"))))) \
            .filter(col("GROUP_NAME").isNotNull() & (col("GROUP_NAME") != "*") & (col("GROUP_NAME") != "")) \
            .withColumn("INPUT_DATE", to_timestamp(col("INPUT_DATE"))) \
            .withColumn("LAST_UPDATE_DATE", to_timestamp(col("LAST_UPDATE_DATE"))) \
            .withColumn("DELETE_DATE", to_timestamp(col("DELETE_DATE"))) \
            .withColumn("IS_MNC", col("IS_MNC").cast(BooleanType())) \
            .withColumn("IS_VIC", col("IS_VIC").cast(BooleanType()))
    elif "CLIENT_RELATIONSHIPS" == table:  # This is a pseudo table that contains a list of relationships to update
        if df is None:
            schema = StructType([
                StructField("from", StringType(), nullable=False),
                StructField("to", StringType(), nullable=False),
                StructField("type", StringType(), nullable=False),
                StructField("verified", StringType(), nullable=True),
                StructField("verified_timestamp", DateType(), nullable=True)
            ])
            df = s.createDataFrame(s.sparkContext.emptyRDD(), schema=schema)
        # Order and de-duplicate the relations. If the latest is a "not_related", any existing relationships
        # should be removed.
        dedup_window_spec = w.Window \
            .partitionBy(col("key_dedup"), col("type")) \
            .orderBy(col("verified_timestamp").desc())
        df = df \
            .withColumn("from", lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("from"))), 16, "0")) \
            .withColumn("to", lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("to"))), 16, "0")) \
            .withColumn("verified", col("verified").cast(BooleanType())) \
            .withColumn("verified_timestamp", _create_multi_format_date_parser("verified_timestamp")) \
            .filter(col("verified") == True) \
            .withColumn("key_dedup", array_join(array_sort(array(col("from"), col("to"))), "")) \
            .withColumn("rank", row_number().over(dedup_window_spec)) \
            .filter(col("rank") == 1) \
            .select(col("from"), col("to"), col("type"), col("verified_timestamp")) \
            .repartition(s._sc.defaultParallelism, col("from")) \
            .cache()
    elif "OCSMEXHG" == table:  # Exchange rates
        df = df \
            .repartition(1, col("CURRENCY_CODE")) \
            .filter(col("CURRENCY_CODE").isNotNull()
                    & (col("CURRENCY_CODE") != "")
                    & (col("CURRENCY_CODE") != "RMB")
                    & (col("COMPANY_CODE") == COMPANY_CODE)
                    & (col("DELETE_DATE").isNull())) \
            .select(col("CURRENCY_CODE"), col("BASE_CURRENCY_CODE"), col("START_FORTNIGHT"),
                    col("NOMINATOR").cast(exchange_rate_precision), col("DENOMINATOR").cast(exchange_rate_precision))
    elif "OCSMGPFN" == table:  # Fortnight codes
        df = df \
            .repartition(1, col("FORTNIGHT_CODE")) \
            .filter((col("COMP_GRP_CODE") == COMPANY_CODE)
                    & col("FORTNIGHT_CODE").isNotNull()
                    & (col("FORTNIGHT_CODE") != "")
                    & (col("COMP_GRP_CODE") == COMPANY_CODE)) \
            .drop(col("COMP_GRP_CODE")) \
            .withColumn("FISCAL_YEAR", col("FISCAL_YEAR").cast(IntegerType())) \
            .withColumn("ACC_PERIOD", col("ACC_PERIOD").cast(IntegerType())) \
            .withColumn("START_DATE", to_date(to_timestamp(col("START_DATE")))) \
            .withColumn("END_DATE", to_date(to_timestamp(col("END_DATE"))))
    elif "OCSMENAG" == table:  # Engagement codes (this is a big table)
        df = df \
            .repartition(s._sc.defaultParallelism, col("CLIENT_CODE")) \
            .withColumn("FINANCIAL_YEAR", col("FINANCIAL_YEAR").cast(IntegerType())) \
            .filter((col("COMPANY_CODE") == COMPANY_CODE)
                    & col("CLIENT_CODE").isNotNull() & col("ENG_CODE").isNotNull() & col("OFFICE_CODE").isNotNull()
                    & (substring(col("CLIENT_CODE"), 0, 1) != "9")
                    & (col("FINANCIAL_YEAR").between(2012, year(add_months(current_date(), 3))))
                    & (col("DELETE_DATE").isNull())) \
            .withColumn("KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME),
                                          col("CLIENT_CODE"), col("ENG_CODE"))), 16, "0")) \
            .withColumn("CLIENT_CODE_KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("CLIENT_CODE"))), 16, "0")) \
            .withColumn("PARTNER_KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("PARTNER"))), 16, "0")) \
            .withColumn("MANAGER_KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("MANAGER"))), 16, "0")) \
            .withColumn("SUPERVISOR_KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("SUPERVISOR"))), 16, "0")) \
            .withColumn("INPUT_DATE", to_timestamp(col("INPUT_DATE"))) \
            .withColumn("LAST_UPDATE_DATE", to_timestamp(col("LAST_UPDATE_DATE"))) \
            .withColumn("DELETE_DATE", to_timestamp(col("DELETE_DATE"))) \
            .withColumn("START_DATE", to_date(to_timestamp(col("START_DATE")))) \
            .withColumn("PLAN_RECOVERY_RATE", col("PLAN_RECOVERY_RATE").cast(IntegerType()))
    elif "OCSSENAG" == table:  # Engagement codes summary (this is a big table)
        # The only reason to use this is for the close date. Everything else can be obtained from the raw tables
        # more reliably. As such, information will be filtered.
        df = df \
            .repartition(s._sc.defaultParallelism, col("CLIENT_CODE")) \
            .filter((col("COMPANY_CODE") == COMPANY_CODE)
                    & col("CLIENT_CODE").isNotNull() & col("ENG_CODE").isNotNull()
                    & (substring(col("CLIENT_CODE"), 0, 1) != "9")) \
            .withColumn("KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME),
                                          col("CLIENT_CODE"), col("ENG_CODE"))), 16, "0")) \
            .withColumn("CLIENT_KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("CLIENT_CODE"))), 16, "0")) \
            .withColumn("CLOSE_DATE", to_date(to_timestamp(col("CLOSE_DATE")))) \
            .select(col("KEY"), col("CLOSE_DATE"))
    elif "OCSDTSSV" == table:  # Timesheet service details (this is a big table)
        df = df \
            .repartition(s._sc.defaultParallelism, col("CLIENT_CODE")) \
            .filter((col("COMPANY_CODE") == COMPANY_CODE)
                    & col("CLIENT_CODE").isNotNull() & col("ENG_CODE").isNotNull()
                    & (substring(col("CLIENT_CODE"), 0, 1) != "9")
                    & (col("RECORD_TYPE") == "T")) \
            .withColumn("KEY",  # Record key
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("FORTNIGHT_CODE"), col("STAFF_CODE"),
                                          col("RECORD_TYPE"), col("REF_NO"), col("CLIENT_CODE"),
                                          col("ENG_CODE"), col("PROJECT_CODE"))), 16, "0")) \
            .withColumn("CLIENT_CODE_KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("CLIENT_CODE"))), 16, "0")) \
            .withColumn("ENG_CODE_KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME),
                                          col("CLIENT_CODE"), col("ENG_CODE"))), 16, "0")) \
            .withColumn("STAFF_CODE_KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("STAFF_CODE"))), 16, "0")) \
            .withColumn("TOTAL_HRS", col("TOTAL_HRS").cast(decimal_precision)) \
            .withColumn("ADJUST_HRS", col("ADJUST_HRS").cast(decimal_precision)) \
            .withColumn("TOTAL_COST", col("TOTAL_COST").cast(decimal_precision)) \
            .withColumn("ADJUST_COST", col("ADJUST_COST").cast(decimal_precision)) \
            .na.fill(0, ["TOTAL_HRS", "ADJUST_COST", "TOTAL_COST", "ADJUST_COST"])
    elif "OCSCSCAT" == table:  # Staff categories
        df = df \
            .repartition(1, col("COMPANY_CODE")) \
            .filter((col("COMPANY_CODE") == COMPANY_CODE)
                    & col("CATEGORY_CODE").isNotNull()
                    & (trim(col("CATEGORY_CODE")) != "")
                    & (col("CATEGORY_CODE") != "ZZZ")
                    & col("DELETE_DATE").isNull()) \
            .withColumn("KEY",  # Record key
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("CATEGORY_CODE"))), 16, "0")) \
            .withColumn("INPUT_DATE", to_timestamp(col("INPUT_DATE"))) \
            .withColumn("LAST_UPDATE_DATE", to_timestamp(col("LAST_UPDATE_DATE"))) \
            .withColumn("DELETE_DATE", to_timestamp(col("DELETE_DATE")))
    elif "OCSMDEPT" == table:  # Departments
        df = df \
            .repartition(1, col("DEPT_CODE")) \
            .filter(col("DEPT_CODE").isNotNull() &
                    col("DELETE_DATE").isNull()) \
            .withColumn("KEY",  # Record key
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME),
                                          col("DEPT_CODE"), col("OFFICE_CODE"), col("WORK_GRP_CODE"))), 16, "0")) \
            .withColumn("INPUT_DATE", to_timestamp(col("INPUT_DATE"))) \
            .withColumn("LAST_UPDATE_DATE", to_timestamp(col("LAST_UPDATE_DATE"))) \
            .withColumn("DELETE_DATE", to_timestamp(col("DELETE_DATE")))
    elif "OCSMSTAF" == table:  # Staff
        # Records are marked as "deleted" once the staff leaves, however, we need the handle for historical purposes.
        df = df \
            .repartition(1, col("STAFF_CODE")) \
            .filter((col("COMPANY_CODE") == COMPANY_CODE)
                    & col("STAFF_CODE").isNotNull()
                    & (trim(col("STAFF_CODE")) != "")) \
            .withColumn("KEY",  # Record key
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("STAFF_CODE"))), 16, "0")) \
            .withColumn("INPUT_DATE", to_timestamp(col("INPUT_DATE"))) \
            .withColumn("LAST_UPDATE_DATE", to_timestamp(col("LAST_UPDATE_DATE"))) \
            .withColumn("DELETE_DATE", to_timestamp(col("DELETE_DATE"))) \
            .withColumn("JOIN_DATE", to_date(to_timestamp(col("JOIN_DATE")))) \
            .withColumn("TERM_DATE", to_date(to_timestamp(col("TERM_DATE")))) \
            .withColumn("LASTWK_DATE", to_date(to_timestamp(col("LASTWK_DATE")))) \
            .withColumn("IS_TEMP_STAFF", col("IS_TEMP_STAFF").cast(BooleanType())) \
            .withColumn("FULL_NAME",
                        when(trim(col("FIRST_NAME")).endswith("/"),
                             regexp_replace(concat_ws(" ", col("FIRST_NAME"), col("MIDDLE_NAME"), col("LAST_NAME")),
                                            r"\s*/\s*", ", ")
                        ).otherwise(
                            regexp_replace(concat_ws(" ", col("FIRST_NAME"), col("LAST_NAME")), r"\s+", " ")
                        )) \
            .withColumn("JOB_TITLE", when(trim(col("FIRST_NAME")).endswith("/"), None).otherwise(col("JOB_TITLE"))) \
            .drop("PASSWORD", "CREDIT_CARD_NO")
    elif "SERVICE_CODE_MAPPING" == table:  # Service code mapping
        if df is None:
            logger.warn("No service mapping found")
            schema = StructType([
                StructField("pattern", StringType(), nullable=False),
                StructField("service", StringType(), nullable=False),
                StructField("subservice", StringType(), nullable=False),
                StructField("recurring", StringType(), nullable=False),
                StructField("comment", StringType(), nullable=True)
            ])
            df = s.createDataFrame(s.sparkContext.emptyRDD(), schema=schema)
        df = df \
            .repartition(1, col("pattern")) \
            .select(
                col("pattern"), col("service"), col("subservice"), col("recurring").cast(BooleanType())
            ) \
            .na.fill(False, ["recurring"])
    elif "OCSMFENT" == table:  # Invoice header
        df = df \
            .repartition(s._sc.defaultParallelism, col("ISSUE_TO_CLIENT")) \
            .filter((col("COMPANY_CODE") == COMPANY_CODE)
                    & col("FEE_NOTE_NO").isNotNull() & (trim(col("FEE_NOTE_NO")) != "")
                    & (substring(col("ISSUE_TO_CLIENT"), 0, 1) != "9")) \
            .withColumn("KEY",  # Record key
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("FEE_NOTE_NO"))), 16, "0")) \
            .withColumn("ISSUE_DATE", to_date(to_timestamp(col("ISSUE_DATE")))) \
            .withColumn("ISSUE_TO_CLIENT_KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("ISSUE_TO_CLIENT"))), 16, "0")) \
            .withColumn("ISSUE_BY_KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("ISSUE_BY"))), 16, "0")) \
            .withColumn("FOR_STAFF_KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("FOR_STAFF"))), 16, "0")) \
            .withColumn("IS_INTER_FIRM", col("IS_INTER_FIRM").cast(BooleanType())) \
            .withColumn("IS_SEND_REMINDER", col("IS_SEND_REMINDER").cast(BooleanType()))

    elif "OCSIFNHD" == table:  # Invoice web data. Note that this may override the header data because its transient
        df = df \
            .repartition(s._sc.defaultParallelism, col("ISSUE_TO_CLIENT")) \
            .filter((col("COMPANY_CODE") == COMPANY_CODE)
                    & col("FEE_NOTE_NO").isNotNull() & (trim(col("FEE_NOTE_NO")) != "")
                    & (substring(col("ISSUE_TO_CLIENT"), 0, 1) != "9")) \
            .withColumn("KEY",  # Record key
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("FEE_NOTE_NO"))), 16, "0")) \
            .withColumn("ISSUE_DATE", to_date(to_timestamp(col("ISSUE_DATE")))) \
            .withColumn("ISSUE_TO_CLIENT_KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("ISSUE_TO_CLIENT"))), 16, "0")) \
            .withColumn("ISSUE_BY_KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("ISSUE_BY"))), 16, "0")) \
            .withColumn("FOR_STAFF_KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("FOR_STAFF"))), 16, "0")) \
            .withColumn("IS_INTER_FIRM", col("IS_INTER_FIRM").cast(BooleanType())) \
            .withColumn("IS_SEND_REMINDER", col("IS_SEND_REMINDER").cast(BooleanType())) \
            .withColumn("INPUT_DATE", to_timestamp(col("INPUT_DATE"))) \
            .withColumn("INPUT_BY_KEY",
                    lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("INPUT_BY"))), 16, "0")) \
            .withColumn("LAST_UPDATE_DATE", to_timestamp(col("LAST_UPDATE_DATE"))) \
            .withColumn("DELETE_DATE", to_timestamp(col("DELETE_DATE")))

    elif "OCSDFNEG" == table:  # Invoice service detail
        df = df \
            .repartition(s._sc.defaultParallelism, col("CLIENT_CODE")) \
            .filter((col("COMPANY_CODE") == COMPANY_CODE)
                    & col("FEE_NOTE_NO").isNotNull() & (trim(col("FEE_NOTE_NO")) != "")
                    & col("CLIENT_CODE").isNotNull() & (trim(col("CLIENT_CODE")) != "")
                    & col("ENG_CODE").isNotNull() & (trim(col("ENG_CODE")) != "")
                    & (substring(col("CLIENT_CODE"), 0, 1) != "9")) \
            .withColumn("KEY",  # Record key
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME),
                                          col("FEE_NOTE_NO"), col("CLIENT_CODE"), col("ENG_CODE"))), 16, "0")) \
            .withColumn("CLIENT_CODE_KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("CLIENT_CODE"))), 16, "0")) \
            .withColumn("ENG_CODE_KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME),
                                          col("CLIENT_CODE"), col("ENG_CODE"))), 16, "0")) \
            .withColumn("FEE_NOTE_NO_KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("FEE_NOTE_NO"))), 16, "0")) \
            .withColumn("IS_FINAL_FEE", col("IS_FINAL_FEE").cast(BooleanType())) \
            .withColumn("FEE_NOTE_AMT", col("FEE_NOTE_AMT").cast(decimal_precision)) \
            .withColumn("FEE_ADJ", col("FEE_ADJ").cast(decimal_precision)) \
            .withColumn("FEE_RECIPT", col("FEE_RECIPT").cast(decimal_precision)) \
            .withColumn("FEE_CR", col("FEE_CR").cast(decimal_precision)) \
            .withColumn("FEE_WO", col("FEE_WO").cast(decimal_precision)) \
            .withColumn("FEE_BD", col("FEE_BD").cast(decimal_precision)) \
            .withColumn("PREV_YR_FEE_BD", col("PREV_YR_FEE_BD").cast(decimal_precision)) \
            .withColumn("SVC_FEE", col("SVC_FEE").cast(decimal_precision)) \
            .withColumn("DISB_FEE", col("DISB_FEE").cast(decimal_precision)) \
            .withColumn("SVC_ADJ", col("SVC_ADJ").cast(decimal_precision)) \
            .withColumn("DISB_ADJ", col("DISB_ADJ").cast(decimal_precision)) \
            .withColumn("SVC_RECIPT", col("SVC_RECIPT").cast(decimal_precision)) \
            .withColumn("DISB_RECIPT", col("DISB_RECIPT").cast(decimal_precision)) \
            .withColumn("SVC_CR", col("SVC_CR").cast(decimal_precision)) \
            .withColumn("DISB_CR", col("DISB_CR").cast(decimal_precision)) \
            .withColumn("SVC_WO", col("SVC_WO").cast(decimal_precision)) \
            .withColumn("DISB_WO", col("DISB_WO").cast(decimal_precision)) \
            .withColumn("SVC_BD", col("SVC_BD").cast(decimal_precision)) \
            .withColumn("DISB_BD", col("DISB_BD").cast(decimal_precision)) \
            .withColumn("PREV_YR_SVC_BD", col("PREV_YR_SVC_BD").cast(decimal_precision)) \
            .withColumn("PREV_YR_DISB_BD", col("PREV_YR_DISB_BD").cast(decimal_precision))
    elif "OCSDFNPJ" == table:  # Invoice service detail down to the project level.
        df = df \
            .repartition(s._sc.defaultParallelism, col("CLIENT_CODE")) \
            .filter((col("COMPANY_CODE") == COMPANY_CODE)
                    & col("FEE_NOTE_NO").isNotNull() & (trim(col("FEE_NOTE_NO")) != "")
                    & col("CLIENT_CODE").isNotNull() & (trim(col("CLIENT_CODE")) != "")
                    & col("ENG_CODE").isNotNull() & (trim(col("ENG_CODE")) != "")
                    & col("PROJECT_DISBURS").isNotNull() & (trim(col("PROJECT_DISBURS")) != "")
                    & (substring(col("CLIENT_CODE"), 0, 1) != "9")) \
            .withColumn("KEY",  # Record key
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME),
                                          col("FEE_NOTE_NO"), col("CLIENT_CODE"),
                                          col("ENG_CODE"), col("PROJECT_DISBURS"))), 16, "0")) \
            .withColumn("CLIENT_CODE_KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("CLIENT_CODE"))), 16, "0")) \
            .withColumn("ENG_CODE_KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME),
                                          col("CLIENT_CODE"), col("ENG_CODE"))), 16, "0")) \
            .withColumn("FEE_NOTE_NO_KEY",
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME), col("FEE_NOTE_NO"))), 16, "0")) \
            .withColumn("FEE_NOTE_AMT", col("FEE_NOTE_AMT").cast(decimal_precision)) \
            .withColumn("FEE_ADJ", col("FEE_ADJ").cast(decimal_precision)) \
            .withColumn("FEE_RECIPT", col("FEE_RECIPT").cast(decimal_precision)) \
            .withColumn("FEE_CR", col("FEE_CR").cast(decimal_precision)) \
            .withColumn("FEE_WO", col("FEE_WO").cast(decimal_precision)) \
            .withColumn("FEE_BD", col("FEE_BD").cast(decimal_precision)) \
            .withColumn("PREV_YR_FEE_BD", col("PREV_YR_FEE_BD").cast(decimal_precision)) \
            .withColumn("FEE_AMT", col("FEE_AMT").cast(decimal_precision)) \
            .withColumn("ADJ_AMT", col("ADJ_AMT").cast(decimal_precision)) \
            .withColumn("RECIPT_AMT", col("RECIPT_AMT").cast(decimal_precision)) \
            .withColumn("CR_AMT", col("CR_AMT").cast(decimal_precision)) \
            .withColumn("WO_AMT", col("WO_AMT").cast(decimal_precision)) \
            .withColumn("BD_AMT", col("BD_AMT").cast(decimal_precision)) \
            .withColumn("PREV_YR_BD_AMT", col("PREV_YR_BD_AMT").cast(decimal_precision)) \
            .withColumn("WIP_WO_AMT", col("WIP_WO_AMT").cast(decimal_precision))
    elif "STAFF_COST" == table:  # Service code mapping
        if df is None:
            logger.fatal("Staff cost data not found.")
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), full_path)

        eff_from_asc = Window.partitionBy(col("level_code"), col("service_line")).orderBy(col("effective_from").asc())
        eff_to_desc = Window.partitionBy(col("level_code"), col("service_line")).orderBy(col("effective_to").desc())
        df = df \
            .repartition(1, col(df.columns[0])) \
            .withColumn("effective_from", to_date(concat(col("year"), lit("0101")), "yyyyMMdd")) \
            .withColumn("effective_to", date_sub(add_months(col("effective_from"), 12), 1)) \
            .withColumn("base", regexp_replace(col("base"), ",", "").cast(decimal_precision)) \
            .withColumn("benefits", regexp_replace(col("benefits"), ",", "").cast(decimal_precision)) \
            .withColumn("facilities", regexp_replace(col("facilities"), ",", "").cast(decimal_precision)) \
            .withColumn("it", regexp_replace(col("it"), ",", "").cast(decimal_precision)) \
            .withColumn("utilisation",
                        when(col("utilisation").endswith("%"),
                             regexp_replace(col("utilisation"), "%", "").cast(decimal_precision) / 100)
                        .otherwise(col("utilisation").cast(decimal_precision))) \
            .withColumn("loading",
                        when(col("loading").endswith("%"),
                             regexp_replace(col("loading"), "%", "").cast(decimal_precision) / 100)
                        .otherwise(col("loading").cast(decimal_precision))) \
            .withColumn("hours_per_month", regexp_replace(col("hours_per_month"), ",", "").cast(decimal_precision)) \
            .withColumn("rank", row_number().over(eff_from_asc)) \
            .withColumn("effective_from", when(col("rank") == 1, lit(None)).otherwise(col("effective_from"))) \
            .withColumn("rank", row_number().over(eff_to_desc)) \
            .withColumn("effective_to", when(col("rank") == 1, lit(None)).otherwise(col("effective_to"))) \
            .withColumn("key",  # Record key. Do this last after the date normalisation
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME),
                                          col("level_code"), col("service_line"),
                                          col("effective_from"), col("effective_to"))), 16, "0")) \
            .drop("year", "rank")
    elif "OFFICE_CODE_MAPPING" == table:  # Service code mapping
        if df is None:
            logger.fatal("Office code mapping data not found.")
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), full_path)

        df = df \
            .repartition(1, col(df.columns[0])) \
            .withColumn("key",  # Record key.
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME),
                                          col("bu"), col("office"), col("location"))), 16, "0"))

    elif "OCSMHLDY" == table:  # Holiday lists.
        df = df \
            .repartition(s._sc.defaultParallelism, col("COMPANY_CODE")) \
            .filter((col("COMPANY_CODE") == COMPANY_CODE)
                    & (col("DELETE_DATE").isNull())) \
            .withColumn("KEY",  # Record key
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME),
                                          col("HOLIDAY_CODE"), col("PLACE_CODE"),
                                          col("YEAR"), col("HOLIDAY_DATE"))), 16, "0")) \
            .withColumn("YEAR", col("YEAR").cast(IntegerType())) \
            .withColumn("HOLIDAY_DATE", to_date(to_timestamp(col("HOLIDAY_DATE")))) \
            .withColumn("INPUT_DATE", to_timestamp(col("INPUT_DATE"))) \
            .withColumn("LAST_UPDATE_DATE", to_timestamp(col("LAST_UPDATE_DATE"))) \
            .withColumn("DELETE_DATE", to_timestamp(col("DELETE_DATE")))

    elif "OCSCARFM" == table:  # AR Form Type.
        df = df \
            .repartition(1, col("AR_FORM_TYPE")) \
            .withColumn("KEY",  # Record key
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME),
                                          col("AR_FORM_TYPE"))), 16, "0")) \
            .na.fill("N", ["IS_SHOW_VACCOUNT"]) \
            .withColumn("IS_SHOW_VACCOUNT", col("IS_SHOW_VACCOUNT").cast(BooleanType())) \
            .withColumn("APPLY_GST", col("APPLY_GST").cast(BooleanType())) \
            .withColumn("IS_SHOW_BANKCHG", col("IS_SHOW_BANKCHG").cast(BooleanType())) \
            .withColumn("INPUT_DATE", to_timestamp(col("INPUT_DATE"))) \
            .withColumn("LAST_UPDATE_DATE", to_timestamp(col("LAST_UPDATE_DATE"))) \
            .withColumn("DELETE_DATE", to_timestamp(col("DELETE_DATE")))

    elif "BankAccount" == table:  # Bank account numbers.
        df = df \
            .repartition(1, col("BankCode")) \
            .filter((col("DeleteDate").isNull())) \
            .withColumn("KEY",  # Record key
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME),
                                          col("BankCode"), col("AccountCode"))), 16, "0")) \
            .withColumn("IsVA", col("IsVA").cast(BooleanType())) \
            .withColumn("InputDate", to_timestamp(col("InputDate"))) \
            .withColumn("LastUpdateDate", to_timestamp(col("LastUpdateDate"))) \
            .withColumn("DeleteDate", to_timestamp(col("DeleteDate")))

    elif "ClientVirtualAccount" == table:  # Client virtual account numbers.
        df = df \
            .repartition(1, col("ClientCode")) \
            .filter((col("DeleteDate").isNull())) \
            .withColumn("KEY",  # Record key
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME),
                                          col("ClientCode"))), 16, "0")) \
            .withColumn("InputDate", to_timestamp(col("InputDate"))) \
            .withColumn("LastUpdateDate", to_timestamp(col("LastUpdateDate"))) \
            .withColumn("DeleteDate", to_timestamp(col("DeleteDate")))

    elif "OCSMCOMP" == table:  # Company.
        df = df \
            .repartition(1, col("COMPANY_CODE")) \
            .withColumn("KEY",  # Record key
                        lpad(hex(xxhash64(lit(bu), lit(OCS_SYSTEM_NAME),
                                          col("COMPANY_CODE"))), 16, "0")) \
            .withColumn("INPUT_DATE", to_timestamp(col("INPUT_DATE"))) \
            .withColumn("LAST_UPDATE_DATE", to_timestamp(col("LAST_UPDATE_DATE"))) \
            .withColumn("DELETE_DATE", to_timestamp(col("DELETE_DATE")))

    return df
