import geopandas as gpd
import pandas as pd


def configure(context):
    context.stage("synthesis.population.spatial.home.locations")
    context.stage("synthesis.population.spatial.primary.locations")
    context.stage("synthesis.population.spatial.secondary.locations")

    context.stage("synthesis.population.activities")
    context.stage("synthesis.population.sampled")


def execute(context):
    df_home = context.stage("synthesis.population.spatial.home.locations")
    df_work, df_education = context.stage("synthesis.population.spatial.primary.locations")
    df_secondary = context.stage("synthesis.population.spatial.secondary.locations")[0]

    df_persons = context.stage("synthesis.population.sampled")[["person_id", "household_id"]]
    df_locations = context.stage("synthesis.population.activities")[["person_id", "activity_index", "purpose"]]

    # Home locations
    df_home_locations = df_locations[df_locations["purpose"] == "home"]
    df_home_locations = pd.merge(df_home_locations, df_persons, on="person_id")
    df_home_locations = pd.merge(df_home_locations, df_home[["household_id", "geometry"]], on="household_id")
    df_home_locations["destination_id"] = -1
    df_home_locations = df_home_locations[["person_id", "activity_index", "destination_id", "geometry"]]

    # Work locations
    df_work_locations = df_locations[df_locations["purpose"] == "work"]
    df_work_locations = pd.merge(df_work_locations,
                                 df_work[["person_id", "destination_id", "geometry"]],
                                 on="person_id")
    df_work_locations = df_work_locations[["person_id", "activity_index", "destination_id", "geometry"]]

    # Education locations
    df_education_locations = df_locations[df_locations["purpose"] == "education"]
    df_education_locations = pd.merge(df_education_locations,
                                      df_education[["person_id", "destination_id", "geometry"]],
                                      on="person_id")
    df_education_locations = df_education_locations[["person_id", "activity_index", "destination_id", "geometry"]]

    # Secondary locations
    df_secondary_locations = df_locations[~df_locations["purpose"].isin(("home", "work", "education"))].copy()
    df_secondary["activity_index"] = df_secondary["trip_index"] + 1
    df_secondary_locations = pd.merge(df_secondary_locations,
                                      df_secondary[["person_id", "activity_index", "destination_id", "geometry"]],
                                      on=["person_id", "activity_index"], how="left")
    df_secondary_locations = df_secondary_locations[["person_id", "activity_index", "destination_id", "geometry"]]

    # Validation
    initial_count = len(df_locations)
    df_locations = pd.concat([df_home_locations, df_work_locations, df_education_locations, df_secondary_locations])

    df_locations = df_locations.sort_values(by=["person_id", "activity_index"])
    final_count = len(df_locations)

    assert initial_count == final_count

    df_locations = gpd.GeoDataFrame(df_locations, crs="epsg:2056")

    return df_locations
