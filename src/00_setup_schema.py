# Databricks notebook source
spark.sql("CREATE CATALOG IF NOT EXISTS main")
spark.sql("CREATE SCHEMA IF NOT EXISTS main.hackathon")
spark.sql("CREATE VOLUME IF NOT EXISTS main.hackathon.bls_data")
spark.sql("CREATE VOLUME IF NOT EXISTS main.hackathon.population_data")
