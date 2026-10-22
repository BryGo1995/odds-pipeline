# shared/plugins/slate.py
import pendulum

# Game dates are bucketed in Eastern time — matches the
# (commence_time AT TIME ZONE 'America/New_York')::date filter in build_features.
SLATE_TZ = "America/New_York"


def slate_date(context) -> str:
    """Return the YYYY-MM-DD game slate a daily DAG run should target.

    Airflow's `ds` is the *start* of the data interval, i.e. the day before
    the run fires, so using it scores yesterday's (already played) games.
    `data_interval_end` is when the run actually fires, which is the slate
    we want to score before tip-off.
    """
    return pendulum.instance(context["data_interval_end"]).in_timezone(SLATE_TZ).to_date_string()
