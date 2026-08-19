from pyspark.sql import SparkSession

from silver.common import read_bronze_json


ESIOS_DATASETS = [
    "demanda_en_consumo",
    "demanda_medida_discriminacion_horaria_total",
    "demanda_real",
    "demanda_real_suma_generacion",
    "generacion_medida_carbon",
    "generacion_medida_ciclo_combinado",
    "generacion_medida_eolica_terrestre",
    "generacion_medida_gas_natural_cogeneracion",
    "generacion_medida_gas_natural_turbina_vapor",
    "generacion_medida_hidraulica",
    "generacion_medida_nuclear",
    "generacion_medida_otras_renovables",
    "generacion_medida_solar_fotovoltaica",
    "generacion_medida_solar_termica",
    "generacion_medida_total",
    "generacion_medida_total_tipo_produccion",
    "generacion_treal_carbon_nacional",
    "generacion_treal_ciclo_combinado_nacional",
    "generacion_treal_cogeneracion_residuos_nacional",
    "generacion_treal_consumo_bombeo_nacional",
    "generacion_treal_eolica_nacional",
    "generacion_treal_hidraulica_nacional",
    "generacion_treal_nuclear_nacional",
    "generacion_treal_solar_fotovoltaica_nacional",
    "generacion_treal_solar_termica_nacional",
    "generacion_treal_termica_renovable_nacional",
    "potencia_instalada_carbon",
    "potencia_instalada_ciclo_combinado",
    "potencia_instalada_eolica",
    "potencia_instalada_hidraulica",
    "potencia_instalada_nuclear",
    "potencia_instalada_otras_renovables",
    "potencia_instalada_solar_fotovoltaica",
    "potencia_instalada_solar_termica",
    "potencia_instalada_total_renovable",
]


def main():
    spark = (
        SparkSession.builder
        .appName("inspect-esios-bronze")
        .getOrCreate()
    )

    print("=" * 80)
    print("ESIOS BRONZE INSPECTION")
    print("=" * 80)

    for dataset in ESIOS_DATASETS:
        print("=" * 80)
        print(f"DATASET = {dataset}")
        print("=" * 80)

        df = read_bronze_json(
            spark=spark,
            source="esios",
            dataset=dataset,
            multiline=True,
        )

        print("TOP LEVEL COLUMNS =", sorted(df.columns))
        df.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()