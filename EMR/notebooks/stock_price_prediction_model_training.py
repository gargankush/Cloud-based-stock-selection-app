{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {
    "scrolled": true
   },
   "outputs": [
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "f7f1d6d4628d485f95b0ae7f22b5b9f9",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "VBox()"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Starting Spark application\n"
     ]
    },
    {
     "data": {
      "text/html": [
       "<table>\n",
       "<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>Current session?</th></tr><tr><td>4</td><td>application_1604345130372_0005</td><td>pyspark</td><td>idle</td><td><a target=\"_blank\" href=\"http://ip-172-31-78-253.ec2.internal:20888/proxy/application_1604345130372_0005/\">Link</a></td><td><a target=\"_blank\" href=\"http://ip-172-31-74-240.ec2.internal:8042/node/containerlogs/container_1604345130372_0005_01_000001/livy\">Link</a></td><td>✔</td></tr></table>"
      ],
      "text/plain": [
       "<IPython.core.display.HTML object>"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "SparkSession available as 'spark'.\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "from pyspark.sql import SparkSession\n",
    "from datetime import date, timedelta, datetime\n",
    "from pyspark.ml.linalg import Vectors, VectorUDT\n",
    "from pyspark.ml.feature import VectorAssembler\n",
    "from pyspark.ml.regression import LinearRegression\n",
    "from pyspark.sql.functions import col, udf, collect_list, dayofyear\n",
    "from pyspark.sql.types import *\n",
    "\n",
    "\n",
    "bucket_name = \"web-app-project\"\n",
    "spark = SparkSession\\\n",
    "    .builder\\\n",
    "    .appName(\"price_prediction\")\\\n",
    "    .getOrCreate()\n",
    "\n",
    "today = date.today().isoformat()\n",
    "prices_path = \"s3://\" + bucket_name + \"/price-data-\" + today + \".csv\" \n",
    "df = spark.read.csv(prices_path, header=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "metadata": {
    "scrolled": true
   },
   "outputs": [
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "63936d95eea44bb0b470c67cec150c23",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "VBox()"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+------+-------------------+------+-------+------+-------+------+\n",
      "|symbol|          timeframe|  open|   high|   low|  close|volume|\n",
      "+------+-------------------+------+-------+------+-------+------+\n",
      "|  FLIR|2020-04-01 13:29:00| 30.38|30.3871|30.355|30.3871|  1897|\n",
      "|  FLIR|2020-04-01 13:28:00| 30.38|  30.38|  30.3|  30.33|  2698|\n",
      "|  FLIR|2020-04-01 13:27:00| 30.43|  30.44| 30.36|  30.38|  7775|\n",
      "|  FLIR|2020-04-01 13:26:00| 30.43|  30.46|  30.4|  30.42|  2630|\n",
      "|  FLIR|2020-04-01 13:25:00|30.325|  30.42|30.325|  30.39|  2309|\n",
      "|  FLIR|2020-04-01 13:24:00| 30.25|  30.35| 30.25|  30.35|  3129|\n",
      "|  FLIR|2020-04-01 13:23:00|30.215|  30.25| 30.21|  30.25|  3731|\n",
      "|  FLIR|2020-04-01 13:22:00| 30.12|  30.19| 30.12|  30.19|  2970|\n",
      "|  FLIR|2020-04-01 13:21:00| 30.14|  30.15| 30.12|  30.12|  1333|\n",
      "|  FLIR|2020-04-01 13:20:00| 30.11|  30.13| 30.11|  30.11|  1565|\n",
      "|  FLIR|2020-04-01 13:19:00| 30.02| 30.125| 30.01| 30.125|  3383|\n",
      "|  FLIR|2020-04-01 13:18:00| 30.09|  30.12| 30.02|  30.03|  7629|\n",
      "|  FLIR|2020-04-01 13:17:00| 30.12|  30.13| 30.11|  30.11|   663|\n",
      "|  FLIR|2020-04-01 13:16:00| 30.17|  30.17| 30.14|  30.14|   635|\n",
      "|  FLIR|2020-04-01 13:15:00| 30.22|  30.24| 30.17|  30.17|  2202|\n",
      "|  FLIR|2020-04-01 13:14:00| 30.25|  30.25|30.195|   30.2|  1997|\n",
      "|  FLIR|2020-04-01 13:13:00| 30.24|  30.24|  30.2|   30.2|   439|\n",
      "|  FLIR|2020-04-01 13:12:00| 30.22|  30.24| 30.21|  30.21|  1107|\n",
      "|  FLIR|2020-04-01 13:11:00| 30.21|  30.22| 30.18|  30.18|  1418|\n",
      "|  FLIR|2020-04-01 13:10:00| 30.21|  30.22| 30.18|  30.18|  1010|\n",
      "+------+-------------------+------+-------+------+-------+------+\n",
      "only showing top 20 rows"
     ]
    }
   ],
   "source": [
    "# we have minute by minute data\n",
    "df.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "daa67369fffe406d8d8544896233b880",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "VBox()"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "46148447"
     ]
    }
   ],
   "source": [
    "# over a year for 150 stocks\n",
    "df.count()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "e8d7b9d5f39746d2b9848964feb6fb13",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "VBox()"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "# convert column types\n",
    "df = df.withColumn(\"timeframe\", col(\"timeframe\").cast(TimestampType()))\n",
    "for col_name in df.columns[2:]:\n",
    "    df = df.withColumn(col_name, col(col_name).cast(FloatType()))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "3011b6388326440db5ec8128d5510b6d",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "VBox()"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "# extract day of year from timestamp\n",
    "df = df.withColumn(\"dayofyear\", dayofyear(\"timeframe\"))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "ab0565aedf9445bc89f8a20024cf8981",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "VBox()"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "# aggregate by day\n",
    "df = df.groupBy(\"symbol\", \"dayofyear\") \\\n",
    ".agg({\"open\": \"avg\"}) \\\n",
    ".orderBy(\"symbol\", \"dayofyear\", ascending=[1, 1])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "8c67326d7b5d4bb8915ba6beb37fa60f",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "VBox()"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "# remove null rows\n",
    "df = df.filter(df.dayofyear.isNotNull())\n",
    "\n",
    "# create a features column : list of open prices averaged by day\n",
    "output = df.groupby('symbol').agg(collect_list('avg(open)').alias(\"features\"))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "fe12840bc22741da9f3b9e348613f4cd",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "VBox()"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+------+--------------------+------------------+\n",
      "|symbol|            features|    yearly_average|\n",
      "+------+--------------------+------------------+\n",
      "|   ALL|[112.12088938554128]|101.45785856936234|\n",
      "|   BMY| [63.42082037948107]| 60.46307769685911|\n",
      "|  CSCO|  [48.1446674601237]| 43.62785568062368|\n",
      "|  JNPR|[24.663368762447146]|23.243451761090988|\n",
      "|  SPGI| [276.2612751805505]| 308.9092829158356|\n",
      "|   TEL| [96.19516385494893]| 87.21687016986051|\n",
      "|  TTWO|[122.43402517029128]|136.51316238148243|\n",
      "|   TSN| [90.23361413604967]| 68.99426036304392|\n",
      "|  ABMD|[169.75242100591245]|215.77113403068725|\n",
      "|   AXP|[125.81960952491092]|105.83465311852946|\n",
      "|   CMG| [850.7590249551309]| 960.8799267647728|\n",
      "|   DGX|[105.55475975016701]|108.70560963625907|\n",
      "|   GIS|[52.645226509038004]|57.425679998283464|\n",
      "|   FLT|[288.08307684656563]|260.24610334331174|\n",
      "|   FRT|[125.75166911776104]|  97.7972130926789|\n",
      "|   HAS|[104.68668922932943]| 83.62468778130545|\n",
      "|  INFO|  [75.2559786602893]| 73.81775680953817|\n",
      "|   OXY|[42.275031822561424]|23.165705464050546|\n",
      "|   PHM| [38.59898244790014]| 38.23872349787257|\n",
      "|   AOS| [47.46759721303831]|46.524904585809125|\n",
      "+------+--------------------+------------------+\n",
      "only showing top 20 rows"
     ]
    }
   ],
   "source": [
    "# add a yearly average column\n",
    "yearly_avg = udf(lambda x: sum(x)/len(x), DoubleType())\n",
    "output = output.withColumn(\"yearly_average\", yearly_avg(\"features\"))\n",
    "\n",
    "# convert to vectors for the linear regression model\n",
    "array_to_vector = udf(lambda x: Vectors.dense(x[0]), VectorUDT())\n",
    "output = output.withColumn(\"features\", array_to_vector(\"features\"))\n",
    "\n",
    "output.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "15c9aec1dff24a27871fa4f73a2de4d8",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "VBox()"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "lr = LinearRegression(labelCol=\"yearly_average\", maxIter=5, regParam=0.2, elasticNetParam=0.8)\n",
    "\n",
    "train_data, test_data = output.randomSplit([0.8, 0.2])\n",
    "\n",
    "model = lr.fit(train_data)\n",
    "\n",
    "results = model.evaluate(test_data)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "ad87f2e3a8744c19997b71d1d5c6580b",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "VBox()"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+------+--------------------+------------------+------------------+\n",
      "|symbol|            features|    yearly_average|        prediction|\n",
      "+------+--------------------+------------------+------------------+\n",
      "|   ALL|[112.12088938554128]|101.45785856936234|107.60294385865248|\n",
      "|   CMG| [850.7590249551309]| 960.8799267647728| 891.4916065289343|\n",
      "|  JNPR|[24.663368762447146]|23.243451761090988| 14.78759141434742|\n",
      "|   TSN| [90.23361413604967]| 68.99426036304392| 84.37480805222025|\n",
      "|   EQR| [79.89796229771206]| 67.69198634982793|  73.4059724245656|\n",
      "|  INTC|  [60.7035345922141]|56.410302546541864| 53.03565367141665|\n",
      "|  MKTX| [379.5526673453195]|433.43359019498274| 391.4181555099817|\n",
      "|   NOV|[24.983040930044773]|15.388677077269975| 15.12684737187766|\n",
      "|  DXCM| [217.2046298174791]| 317.2740200902268|219.12433544245056|\n",
      "|   SHW| [563.9035714571593]| 587.4116650986135| 587.0627840869374|\n",
      "|  ATVI|[58.944528227870904]| 67.92506071942839|  51.1688868742891|\n",
      "|   CAG| [33.67942974100283]| 32.96103989589461|24.355995268263868|\n",
      "|    KR|[28.591529967216065]| 31.02246362654304|18.956400043738157|\n",
      "|   PLD| [88.34507591555817]| 92.57343755641874| 82.37057400475774|\n",
      "|   SWK|[166.51492489589734]|144.01904665027692|165.32927248988744|\n",
      "|  INCY| [84.31983153988617]| 88.85496818031537|  78.0987346850435|\n",
      "|   MTB|  [164.989579621736]|124.89818143053303|163.71048135320896|\n",
      "|   APD| [233.1260742437644]|249.72807847541995|236.02116107070728|\n",
      "|   DRI|[47.628980203108355]| 91.22646577957734|39.160124557038316|\n",
      "|    ED| [89.09485032607098]| 81.53471332766408| 83.16628117401737|\n",
      "+------+--------------------+------------------+------------------+\n",
      "only showing top 20 rows"
     ]
    }
   ],
   "source": [
    "results.predictions.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "5267ca314d87419189aa3c6c64a9e3b7",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "VBox()"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "model_path = \"s3://\" + bucket_name + \"/lr_model\"\n",
    "model.save(model_path)"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "PySpark",
   "language": "",
   "name": "pysparkkernel"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "python",
    "version": 2
   },
   "mimetype": "text/x-python",
   "name": "pyspark",
   "pygments_lexer": "python2"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
