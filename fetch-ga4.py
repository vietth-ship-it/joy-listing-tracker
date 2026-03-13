"""
Fetch GA4 data for Joy Loyalty app listing and export to JSON.
Run: GOOGLE_APPLICATION_CREDENTIALS=service-account.json python3 fetch-ga4.py

Note: GA4 property 381282244 tracks ALL Avada apps on Shopify App Store.
All data is filtered to hostName=apps.shopify.com AND pagePath=/joyio (Joy only).
"""

import json, os, warnings
from datetime import datetime, timedelta

warnings.filterwarnings("ignore")

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, DateRange, Dimension, Metric,
    FilterExpression, FilterExpressionList, Filter, OrderBy
)

GA4_PROPERTY = "properties/381282244"
DAYS_BACK = 90
OUTPUT_FILE = "ga4-metrics.json"


def joy_page_filter():
    """Filter for Joy Loyalty listing page traffic only."""
    return FilterExpression(
        and_group=FilterExpressionList(expressions=[
            FilterExpression(filter=Filter(
                field_name="hostName",
                string_filter=Filter.StringFilter(
                    value="apps.shopify.com",
                    match_type=Filter.StringFilter.MatchType.EXACT,
                ),
            )),
            FilterExpression(filter=Filter(
                field_name="pagePath",
                string_filter=Filter.StringFilter(
                    value="/joyio",
                    match_type=Filter.StringFilter.MatchType.EXACT,
                ),
            )),
        ])
    )


def joy_install_filter():
    """Filter installs where landingPage = /joyio (Joy-specific installs)."""
    return FilterExpression(filter=Filter(
        field_name="landingPage",
        string_filter=Filter.StringFilter(
            value="/joyio",
            match_type=Filter.StringFilter.MatchType.EXACT,
        ),
    ))


def query(client, start, end, dimensions, metrics, dim_filter=None, limit=10000):
    req = RunReportRequest(
        property=GA4_PROPERTY,
        date_ranges=[DateRange(start_date=start, end_date=end)],
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        limit=limit,
    )
    if dim_filter:
        req.dimension_filter = dim_filter
    if dimensions:
        req.order_bys = [OrderBy(
            dimension=OrderBy.DimensionOrderBy(dimension_name=dimensions[0])
        )]

    resp = client.run_report(req)
    rows = []
    for row in resp.rows:
        r = {}
        for i, d in enumerate(dimensions):
            r[d] = row.dimension_values[i].value
        for i, m in enumerate(metrics):
            val = row.metric_values[i].value
            try:
                r[m] = int(val) if "." not in val else round(float(val), 2)
            except ValueError:
                r[m] = val
        rows.append(r)
    return rows


def main():
    client = BetaAnalyticsDataClient()
    today = datetime.now().date()
    end = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    start = (today - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%d")
    prev_end = (today - timedelta(days=DAYS_BACK + 1)).strftime("%Y-%m-%d")
    prev_start = (today - timedelta(days=DAYS_BACK * 2)).strftime("%Y-%m-%d")

    jf = joy_page_filter()
    jif = joy_install_filter()
    traffic_metrics = ["sessions", "totalUsers", "newUsers"]
    install_metrics = ["keyEvents:shopify_app_install"]

    print(f"Fetching GA4 data (Joy only): {start} to {end}...")

    data = {
        "meta": {
            "app": "Joy Loyalty",
            "listing": "apps.shopify.com/joyio",
            "ga4_property": "381282244",
            "period": {"start": start, "end": end},
            "prev_period": {"start": prev_start, "end": prev_end},
            "generated_at": datetime.now().isoformat(),
            "notes": {
                "traffic": "Filtered to hostName=apps.shopify.com AND pagePath=/joyio",
                "installs": "Filtered by landingPage=/joyio — covers installs from listing page visitors only"
            }
        },

        # === TRAFFIC (filtered to /joyio) ===
        "traffic_daily": query(client, start, end,
            ["date"], traffic_metrics, jf),
        "traffic_daily_prev": query(client, prev_start, prev_end,
            ["date"], traffic_metrics, jf),
        "traffic_by_source": query(client, start, end,
            ["sessionSourceMedium"], traffic_metrics, jf),
        "traffic_by_country": query(client, start, end,
            ["country"], traffic_metrics, jf),
        "traffic_by_campaign": query(client, start, end,
            ["sessionCampaignName"], traffic_metrics, jf),
        "traffic_by_keyword": query(client, start, end,
            ["sessionGoogleAdsKeyword"], traffic_metrics, jf),
        "traffic_by_utm": query(client, start, end,
            ["sessionManualSource", "sessionManualMedium",
             "sessionManualCampaignName", "sessionManualTerm"],
            traffic_metrics, jf),
        "traffic_daily_by_source": query(client, start, end,
            ["date", "sessionSourceMedium"], ["sessions"], jf, 5000),
        "traffic_daily_by_country": query(client, start, end,
            ["date", "country"], ["sessions"], jf, 5000),

        # === INSTALLS (filtered by landingPage=/joyio) ===
        "installs_daily": query(client, start, end,
            ["date"], install_metrics, jif),
        "installs_daily_prev": query(client, prev_start, prev_end,
            ["date"], install_metrics, jif),
        "installs_by_source": query(client, start, end,
            ["firstUserSourceMedium"], install_metrics, jif),
        "installs_by_country": query(client, start, end,
            ["country"], install_metrics, jif),
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(data, f, indent=2)

    t_sessions = sum(r["sessions"] for r in data["traffic_daily"])
    t_installs = sum(r["keyEvents:shopify_app_install"] for r in data["installs_daily"])
    print(f"Done! {OUTPUT_FILE} ({os.path.getsize(OUTPUT_FILE)/1024:.0f} KB)")
    print(f"  Period: {start} → {end}")
    print(f"  Joy listing sessions: {t_sessions:,}")
    print(f"  Joy installs (from listing): {t_installs:,}")


if __name__ == "__main__":
    main()
