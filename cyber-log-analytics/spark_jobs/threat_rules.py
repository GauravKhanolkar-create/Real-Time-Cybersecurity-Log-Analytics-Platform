"""
Rule-based threat detection functions.

These operate on a Spark DataFrame of parsed log events (already windowed
/ aggregated as needed) and annotate each row with any rule violations.
Each function returns a Spark Column of type string (rule name) or null.
"""
from pyspark.sql import functions as F
from pyspark.sql import Column


def flag_brute_force(df, threshold: int):
    """
    df must contain a 'failed_login_count_5m' column (rolling count of
    failed logins per src_ip/user within the trailing window, computed
    upstream via a windowed aggregation).
    """
    return df.withColumn(
        "rule_brute_force",
        F.when(F.col("failed_login_count_5m") >= threshold, F.lit("BRUTE_FORCE_LOGIN"))
        .otherwise(F.lit(None)),
    )


def flag_port_scan(df, threshold: int):
    """
    df must contain 'unique_ports_contacted_1m' (rolling distinct dst_port
    count per src_ip within the trailing window).
    """
    return df.withColumn(
        "rule_port_scan",
        F.when(F.col("unique_ports_contacted_1m") >= threshold, F.lit("PORT_SCAN"))
        .otherwise(F.lit(None)),
    )


def flag_large_transfer(df, byte_threshold: int):
    return df.withColumn(
        "rule_large_transfer",
        F.when(
            (F.col("bytes_sent") + F.col("bytes_received")) >= byte_threshold,
            F.lit("LARGE_DATA_TRANSFER"),
        ).otherwise(F.lit(None)),
    )


def flag_blacklisted_ip(df, blacklist):
    return df.withColumn(
        "rule_blacklisted_ip",
        F.when(F.col("src_ip").isin(blacklist), F.lit("BLACKLISTED_IP")).otherwise(F.lit(None)),
    )


def apply_all_rules(df, cfg: dict):
    """
    cfg is the 'detection' section of config.yaml. Applies every rule and
    consolidates the results into a single 'triggered_rules' array column
    and a 'rule_severity' column.
    """
    df = flag_brute_force(df, cfg["failed_login_threshold"])
    df = flag_port_scan(df, cfg["port_scan_unique_ports_threshold"])
    df = flag_large_transfer(df, cfg["large_transfer_bytes_threshold"])
    df = flag_blacklisted_ip(df, cfg["blacklisted_ips"])

    rule_cols = ["rule_brute_force", "rule_port_scan", "rule_large_transfer", "rule_blacklisted_ip"]

    df = df.withColumn(
        "triggered_rules",
        F.array_except(F.array(*[F.col(c) for c in rule_cols]), F.array(F.lit(None).cast("string"))),
    )

    df = df.withColumn(
        "rule_severity",
        F.when(F.array_contains(F.col("triggered_rules"), "BRUTE_FORCE_LOGIN"), F.lit("high"))
        .when(F.array_contains(F.col("triggered_rules"), "PORT_SCAN"), F.lit("high"))
        .when(F.array_contains(F.col("triggered_rules"), "BLACKLISTED_IP"), F.lit("critical"))
        .when(F.array_contains(F.col("triggered_rules"), "LARGE_DATA_TRANSFER"), F.lit("medium"))
        .otherwise(F.lit("none")),
    )

    return df.drop(*rule_cols)
