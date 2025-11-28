# Proyecto data engineering

Este proyecto procesa y transforma datos hospitalarios reales como pacientes, doctores, citas, tratamientos y facturación.
Los datos se ingieren y gestionan en **Snowflake**, y se modelan mediante **dbt** siguiendo una arquitectura profesional.
Las capas Silver y Gold permiten obtener una visión limpia, normalizada y analítica del hospital.
Finalmente, un dashboard en **Power BI** muestra métricas clave como ingresos, actividad médica y eficiencia operativa.


## 📘 Dataset de origen (Kaggle)

Los datos crudos utilizados en este proyecto provienen del dataset público:

**Hospital Management Dataset**  
https://www.kaggle.com/datasets/kanakbaghel/hospital-management-dataset

Este dataset contiene información clínica y operativa relacionada con pacientes, doctores, citas, facturación y tratamientos dentro de un entorno hospitalario.

---

## 📂 Tablas en la capa Bronze

La capa **Bronze** contiene los datos tal cual provienen de Kaggle, sin transformaciones, y sirve como punto de partida del pipeline.

### **🧑‍⚕️ patients**
Contiene información demográfica y médica básica de los pacientes:
- Nombre, apellidos, contacto, dirección  
- Género y fecha de nacimiento  
- Seguro médico  
- Alergias, condiciones crónicas y medicación actual  

### **👨‍⚕️ doctors**
Incluye información del personal médico:
- Nombre, apellido, email, teléfono  
- Especialización  
- Años de experiencia  
- Sucursal del hospital  
- Sala de consulta  
- Días disponibles  

### **📅 appointments**
Registra todas las citas programadas:
- Fecha y hora  
- Motivo de la visita  
- Estado (Scheduled, Completed, Cancelled, No-show)  
- Tiempos de espera, check-in y check-out  
- Duración de la cita  

### **💸 billing**
Contiene datos de facturación hospitalaria:
- Monto de la factura  
- Método de pago  
- Estado del pago  
- Cobertura del seguro  
- Penalización por retraso  
- Fechas clave (bill_date, payment_date, due_date)

### **💉 treatments**
Información detallada de los tratamientos realizados:
- Tipo de tratamiento  
- Coste  
- Equipamiento usado  
- Resultado del tratamiento  
- Duración  
- Riesgos y complicaciones  

---

## 🔧 Enriquecimiento de los datos

Para mejorar la calidad analítica del dataset, se han añadido nuevas columnas en los modelos posteriores (staging/silver).

---

## 🥉 1. Bronze Layer — Raw Ingestion

- La capa Bronze contiene los datos exactamente como vienen en los archivos originales del dataset (CSV):

- Carga mediante Snowflake Stages + COPY INTO.

- Estructura idéntica a los ficheros.
- 

A continuación se muestra el diagrama ER que representa la estructura original del dataset en la capa Bronze:

![Bronze Diagram](./Bronce.png)

---

## 🥈 Hospital Data Engineering Project — Staging (Silver) Layer

En la arquitectura **Medallion**, esta etapa corresponde a la **capa Silver**, donde los datos pasan de estar en bruto a estar **limpios, estandarizados y enriquecidos**.

En nuestro proyecto, siguiendo las buenas prácticas de **dbt**, esta misma capa se implementa mediante los modelos dentro de la carpeta **`staging/`**.

---

## 🎯 Objetivos de la capa Staging/Silver

- Estandarización de formatos (fechas, textos, códigos)  
- Normalización de categorías, estados y nomenclaturas  
- Creación de columnas derivadas necesarias para el negocio  
- Resolución de duplicados e inconsistencias  
- Generación de **surrogate keys** para el modelo dimensional  
- Enriquecimiento semántico previo a la capa de hechos y dimensiones  

El resultado es un conjunto de tablas limpias, confiables y consistentes.

---

## 🧪 Tests genéricos aplicados en la capa Staging (ejemplos)

En la capa Staging se aplican tests genéricos de dbt para asegurar calidad, coherencia y consistencia de los datos.  
Algunos ejemplos:

- **unique**:  

- **not_null**:  

- **relationships**:  

- **accepted_values**:  

- **tests propios del proyecto** (ejemplos):
  - Validación de fechas futuras.  
  - Validación de que duración ≥ 0.  
  - Flags booleanos en {0,1}.

---

## 🧰 Macros utilizadas (ejemplos)

Se emplean macros auxiliares para mantener consistencia y reutilizar lógica:

- **`generate_surrogate_key()`** 

![Silver Diagram](./Silver.png)

---
## 🥇 3. Gold Layer — Modelado Dimensional (Star Schema)

La capa **Gold** representa el modelo de negocio final, siguiendo las mejores prácticas de modelado dimensional (Kimball).
Aquí los datos ya no solo están limpios: ahora están organizados específicamente para análisis y reporting.

### 📚 Dimensiones creadas

Se han generado varias dimensiones basadas en las tablas Silver:

- dim_patients

- dim_doctors

- dim_date

- dim_payment_method

- dim_treatment_type

- dim_appointment_status

- dim_insurance_provider

Estas tablas contienen información descriptiva, estandarizada y sin duplicados.

### 📦 Tablas de hechos

Las métricas y eventos del hospital se recopilan en tablas de hechos:

- fct_appointments
  
- fct_treatments
  
- fct_billing


La granularidad de cada tabla está definida por evento: cita, tratamiento o factura.

![GOLD](./Gold.png)

## 📈 Caso de uso final: análisis del tiempo promedio de espera

Como aplicación práctica del pipeline, se desarrolló un análisis completo del **tiempo promedio de espera** de los pacientes:

- Tiempo medio por doctor

- Tiempo medio por especialidad

- Tiempo medio por día de la semana

## 📊 Dashboard en Power BI

Finalmente, los modelos Gold se conectan a un dashboard en Power BI, donde se visualizan métricas clave:

![PowerBI](./PowerBI.jpg)
