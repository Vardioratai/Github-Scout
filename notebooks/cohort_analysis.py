import polars as pl
import plotly.express as px

def plot_score_distribution_by_cohort(df: pl.DataFrame):
    """1. Plot score distribution (histogram) broken down by age and maturity tiers."""
    required_cols = ["potential_score", "age_tier", "maturity_tier"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' is missing. Have you calculated scores yet?")
            
    # Drop rows without a score just for visualization
    viz_df = df.drop_nulls("potential_score").to_pandas()
    
    fig = px.histogram(
        viz_df,
        x="potential_score",
        facet_col="maturity_tier",
        facet_row="age_tier",
        color="maturity_tier",
        title="📊 Score Distribution by Cohorts",
        labels={"potential_score": "Potential Score"},
        template="plotly_dark",
        category_orders={
            "age_tier": ["Emerging", "Growing", "Established"],
            "maturity_tier": ["Seed", "Traction", "Scale"]
        },
        height=700
    )
    # Give a bit of breathing room to the subplots
    fig.update_layout(margin=dict(t=80, b=40, l=40, r=40))
    return fig


def plot_hidden_gems_heatmap(df: pl.DataFrame):
    """2. Plot a heatmap of median scores for 'Emerging' + 'Seed/Traction' repos grouped by month and language."""
    filtered_df = df.filter(
        (pl.col("age_tier") == "Emerging") &
        (pl.col("maturity_tier").is_in(["Seed", "Traction"])) & 
        (pl.col("primary_language").is_not_null()) & 
        (pl.col("created_at").is_not_null())
    )
    
    if filtered_df.height == 0:
        print("No data available for Emerging + Seed/Traction cohorts.")
        return None
        
    # Extract year-month to group by
    aggregated = (
        filtered_df.with_columns(
            pl.col("created_at").dt.strftime("%Y-%m").alias("created_month")
        )
        .group_by(["primary_language", "created_month"])
        .agg([
            pl.col("potential_score").median().alias("median_score"),
            pl.count().alias("repo_count")
        ])
        .filter(pl.col("repo_count") >= 3) # Filter out noise (adjust threshold if necessary)
    )
    
    if aggregated.height == 0:
        print("Not enough data to form a heatmap (repo_count < 3 in groups).")
        return None

    heatmap_df = aggregated.to_pandas().pivot(
        index="primary_language", 
        columns="created_month", 
        values="median_score"
    )
    
    fig = px.imshow(
        heatmap_df,
        labels=dict(x="Creation Month", y="Language", color="Median Score (0-100)"),
        title="🔥 Hidden Gems Heatmap (Emerging + Seed/Traction)",
        text_auto=".1f",
        aspect="auto",
        template="plotly_dark",
        color_continuous_scale="Inferno",
        height=600
    )
    return fig


def plot_segmented_leaderboard(df: pl.DataFrame):
    """3. Plot top 15 Outperformers in Emerging/Growing and Seed/Traction cohorts."""
    top_gems = (
        df.filter(
            (pl.col("age_tier").is_in(["Emerging", "Growing"])) &
            (pl.col("maturity_tier").is_in(["Seed", "Traction"])) &
            (pl.col("potential_score").is_not_null())
        )
        .sort("potential_score", descending=True)
        .head(15)
    )
    
    if top_gems.height == 0:
        print("No repos found matching the criteria.")
        return None
        
    fig = px.bar(
        top_gems.to_pandas(),
        x="potential_score",
        y="full_name",
        orientation="h",
        color="star_velocity",
        hover_data=["stars", "age_tier", "maturity_tier", "description"],
        title="🏆 Top 15 Outperformers (Emerging/Growing & Seed/Traction)",
        labels={"potential_score": "Potential Score", "full_name": "Repository"},
        template="plotly_dark",
        color_continuous_scale="Magma",
        height=600
    )
    # Order by score ascendingly so that the largest bar is visually at the top
    fig.update_layout(yaxis={'categoryorder':'total ascending'})
    return fig


def plot_traction_momentum(df: pl.DataFrame):
    """4. Plot Total Stars vs 7-day Momentum to identify breakout repositories."""
    viz_df = df.filter(
        (pl.col("stars") > 0) & 
        (pl.col("momentum_7d").is_not_null()) & 
        (pl.col("star_velocity").is_not_null())
    )
    
    fig = px.scatter(
        viz_df.to_pandas(),
        x="stars",
        y="momentum_7d",
        size="star_velocity",
        color="age_tier",
        hover_name="full_name",
        hover_data=["potential_score", "stars", "forks", "maturity_tier"],
        log_x=True,
        title="🚀 Traction Momentum: Stars (Log) vs 7-Day Star Growth",
        labels={
            "stars": "Total Stars (Log Scale)", 
            "momentum_7d": "7-Day Momentum Delta",
            "age_tier": "Age Tier",
            "star_velocity": "Star Velocity"
        },
        template="plotly_dark",
        category_orders={"age_tier": ["Emerging", "Growing", "Established"]},
        size_max=40,
        height=700
    )
    
    # Add horizontal line for 0 baseline growth to easily visualize positive/negative momentum
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.8)
    return fig

