# Financial Fraud Detection Service on Databricks

Associated with EC 528 at Boston University  
Group 11

The respository created for our class work can be found at this link: [mlops-with-databricks](https://github.com/EC-CS-528-BU-Cloud-Computing/mlops-with-databricks)  
This is a secondary repo where we can develop and build operations structure without cluttering our main project repository.

## Purpose

This repo is a place for our group to build a financial fraud detection service via Databricks and AWS.

## Goal

Our goal with this repository is to build a machine learning service on Databricks that can detect financial fraud from bank transactions. The focus of our project was the operations that allow developers to build a financial fraud detection system. We focused on the code base to support fraud detection not the machine learning model development.


We setup a basic machine learning model to detect financial fraud but did not worry about the quality of the service. Our goal was not to develop a novel and high-performing fraud detection model. We used a simple decision tree model, which helped keep computation costs low and made it easy to write training, validation, and testing procedures. Instead, we focused on using this model as an example so that we could develop an operations structure that could be easily adapted to other models. Additionally, we weren't concerned with working with real data. Thus, we used a single table with synthetic financial transaction data. If we ever needed a mock data source, we extracted a portion of this table to generate a "new" data source.

The kinds of things we spent the most time developing...
* **Tests** that ensure code and model quality during the model development process
* **Modular workflows** that allow model- and data-specific code to be easily modified without requiring significant changes the service code base
* **Environment configuration** that allowed computation resources to be created to run automated tasks supporting model & code development
* **Model deployment** that made the service available to those outside of the developer workspace on Databricks

## Description of the Service

**NOTE**: We worked on this service in two separate workspaces - (1) Databricks-AWS and (2) Databricks-Azure workspaces. **_This image and description are specific to the Databricks-AWS work_**.

![Databricks-AWS Financial Fraud Detection Service](images/ec528_ml_service_diagram_new.png)


