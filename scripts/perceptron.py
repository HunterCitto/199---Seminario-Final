import numpy as np
import matplotlib.pyplot as plt

class Perceptron() :

    def __init__(self, input_size, learning_rate = 0.01) :
        self.w = np.random.randn(input_size, 1)
        self.b = np.random.randn(1)
        self.learning_rate = learning_rate

        self.error_history = []
        self.accuracy_history = []
        self.training_data = None    

    def evaluar_cota(self, x, y, w, steps) :

        R = np.max(np.linalg.norm(x, axis = 1))

        margins = y * (x @ w).flatten()
        rho = np.min(np.abs(margins))

        nsq_w = np.linalg.norm(w) ** 2

        bound = (R ** 2) / (rho ** 2) * nsq_w

        print("\n" + "="*50)
        print("EVALUACIÓN DE LA COTA TEÓRICA")
        print("="*50)
        print(f"R (máx norma de x): {R:.4f}")
        print(f"ρ (margen mínimo): {rho:.4f}")
        print(f"||w*||^2: {nsq_w:.4f}")
        print(f"Cota teórica: {bound:.2f}")
        print(f"¿Se cumple? {'Sí' if steps <= bound else 'No'}")

    def linear_sep_data(n = 100, p = 2):
        x = np.random.uniform(-1, 1, (n, p))
        w = np.ones((p, 1))
        y = np.where((x @ w) >= 0, 1, -1)
        return x, y.flatten(), w

    def predict(self, x) :
        z = np.dot(x, self.w) + self.b
        return np.where(z >= 0, 1, -1)

    def train(self, x , y, epochs = 100, resultados = False) :

        self.training_data = (x, y)
        convergence_epoch = epochs

        # Información inicial del entrenamiento.
        if resultados:
            print("\n" + "="*50)
            print("INICIO DEL ENTRENAMIENTO")
            print("="*50)
            print(f"Muestras: {len(x)}, Características: {x.shape[1]}")
            print(f"Tasa de aprendizaje: {self.learning_rate}")
            print(f"Pesos iniciales: {self.w.flatten().round(4)}")
            print(f"Bias inicial: {self.b[0]:.4f}")
            print("-" * 50)
        
        for epoch in range(epochs) : # Por cada epoca...
            
            # Contadores de errores y aciertos.
            errors = 0
            correct = 0

            # Por cada muestra/registro...
            for i in range(len(x)) :
                y_pred = self.predict(x[i]) # Se predice la clase.
                error = y[i] - y_pred # En base a la predicción se determina el error / residuo.

                if error != 0 : # Si es diferente de cero
                    self.w += self.learning_rate * error * x[i].reshape(-1, 1) # Ajusto el vector de pesos y...
                    self.b += self.learning_rate * error # el valor de bias.
                    errors += 1 # Sumo el error al contador.
                else : # Sino...
                    correct += 1 # Sumo el acierto al contador.

            accuracy = correct / len(x) # Calculo la precisión.
            self.error_history.append(errors) # Registro el número de errores.
            self.accuracy_history.append(accuracy) # Registro la precisión.

            if resultados and (epoch % 20 == 0 or epoch == epochs-1 or errors == 0) :
                print(f"Época {epoch:3d}: Errores = {errors:3d}, Precisión = {accuracy:.1%}")

            if errors == 0 :
                convergence_epoch = epoch + 1
                if resultados :
                    print(f"¡Convergencia alcanzada en época {convergence_epoch}!")
                    break

        if resultados :
            self.print_results(x, y, convergence_epoch)

        return convergence_epoch

    ### Funciones de visualización y reporte de resultados ###
    def print_results(self, x, y, convergence_epoch):
        n = len(x)

        final_errors = self.error_history[-1] if self.error_history else n
        final_accuracy = (n - final_errors) / n

        print("\n" + "="*50)
        print("RESULTADOS DEL ENTRENAMIENTO")
        print("="*50)
        print(f"Épocas entrenadas: {convergence_epoch}")
        print(f"Precisión final: {final_accuracy:.1%}")
        print(f"Pesos finales: {self.w.flatten().round(4)}")
        print(f"Bias final: {self.b[0]:.4f}")
        
        # Matriz de confusión simplificada
        # y_pred = np.array([self.predict(x_i) for x_i in x]).flatten()
        y_pred = self.predict(x).flatten()
        print(f"Predicciones: {y_pred.shape}, Verdaderos: {y.shape}")
        correct = np.sum(y_pred == y)
        incorrect = n - correct
        
        print(f"\nMatriz de confusión simplificada:")
        print(f"Correctas: {correct}/{n}")
        print(f"Incorrectas: {incorrect}/{n}")

    def plot_training_history(self):
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
        
        # Gráfico de errores
        ax1.plot(self.error_history, 'r-', alpha=0.7)
        ax1.set_title('Evolución de Errores por Época')
        ax1.set_xlabel('Época')
        ax1.set_ylabel('Número de Errores')
        ax1.grid(True, alpha=0.3)
        
        # Gráfico de precisión
        ax2.plot(self.accuracy_history, 'g-', linewidth=2, label='Precisión')
        ax2.set_title('Precisión durante Entrenamiento')
        ax2.set_xlabel('Época')
        ax2.set_ylabel('Precisión')
        ax2.grid(True, alpha=0.3)
        ax2.tick_params(axis='both', which='major', labelsize=10)
        
        plt.tight_layout()
        plt.show()
        
        # Si es un problema 2D, mostrar la frontera de decisión
        if self.w.shape[0] == 2 and self.training_data is not None:
            self.plot_decision_boundary()

    def plot_decision_boundary(self):


        if self.training_data is None:
            print("No hay datos de entrenamiento para visualizar.")
            return
            
        x_train, y_train = self.training_data

        plt.figure(figsize=(8, 6))
        
        # Crear una malla para visualizar la frontera de decisión
        x_min, x_max = -1.5, 1.5
        y_min, y_max = -1.5, 1.5
        xx, yy = np.meshgrid(np.arange(x_min, x_max, 0.01),
                             np.arange(y_min, y_max, 0.01))
        
        # Predecir para cada punto de la malla
        Z = self.predict(np.c_[xx.ravel(), yy.ravel()])
        Z = Z.reshape(xx.shape)
        
        # Graficar contorno
        plt.contourf(xx, yy, Z, alpha=0.3, cmap=plt.cm.coolwarm)
        plt.colorbar()
        
        # Graficar puntos de datos
        plt.scatter(x_train[:, 0], x_train[:, 1], c=y_train, cmap=plt.cm.coolwarm, 
                   edgecolors='k', marker='o')
        
        plt.title('Frontera de Decisión del Perceptrón')
        plt.xlabel('Característica 1')
        plt.ylabel('Característica 2')
        plt.grid(True, alpha=0.3)
        plt.show()