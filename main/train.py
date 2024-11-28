# Import necessary libraries
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import pickle
import time

# Load dataset (replace 'your_dataset.csv' with your actual dataset)
data = pd.read_csv('data.csv')

# Split the data into features (X) and target (y)
# Assume the last column is the target variable
X = data.iloc[:, 1:]
y = data.iloc[:, 0]

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Create a linear regression model
model = LinearRegression()

# Train the model using the training data
model.fit(X_train, y_train)

# Make predictions using the testing data
y_pred = model.predict(X_test)

# Evaluate the model
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

time.sleep(300)

# Persist the model as a .pkl file
model_filename = 'linear_model.pkl'
with open(model_filename, 'wb') as file:
    pickle.dump(model, file)

# Create a DataFrame with the two floats
df_dict = {'mse': [mse], 'r2': [r2]}
df_test = pd.DataFrame(df_dict)
df_test = df_test.transpose()
df_test.to_csv('test_results.csv', index=True)

