# Python Descriptive Statistics Project (Pandas)

![Python CI](https://github.com/aghakishiyeva/ids706-mini-project-2/actions/workflows/main.yml/badge.svg)

This project is focused on calculating descriptive statistics utilizing a Python setup with CI/CD implemented through GitHub Actions. It comes with a **.devcontainer** setup for streamlined development within a Docker container using Visual Studio Code. 
 
## 🚀 Getting Started

### Local Development with VS Code and .devcontainer
1. Ensure **Docker** is installed and running on your machine.
2. Install the **Remote - Containers** extension in VS Code.
3. Clone the repository to your local machine.
4. Open the repository in VS Code.
5. A notification will appear suggesting you reopen the project in a container. Click "Reopen in Container". (Alternatively, press **F1**, type "Remote-Containers: Reopen Folder in Container", and press Enter.)
6. Initially, Docker will build an image based on the defined **Dockerfile**. This step might take a few minutes.
7. Once inside the container, a Python environment, isolated from your local system, is prepared for your use.

### 🧪 Running Tests

To initiate tests in this project, execute the following command:

```bash
pytest
```
 
This command runs all the unit tests and ensures that any changes made haven't introduced new issues.

## 🛠️ File Structure
* src/main.py: The central script housing functions such as read_dataset, generate_summary_statistics, and create_data_visualization.
* src/winequality-red.csv: A dataset file utilized within the project.
* tests/test_main.py: Includes unit tests for the functions defined in main.py.
* tests/test.csv: A CSV file used for testing purposes.
* requirements.txt: Lists the Python dependencies necessary for this project.
* .devcontainer: Contains configurations for the VS Code remote container development setup.
* .github/workflows/main.yml: Describes the workflow procedures for Continuous Integration using GitHub Actions.

## ✨ Contributing
1. Fork the repository.<br>
2. Create a new branch for your contributions.<br>
3. Implement your changes.<br>
4. Run tests using pytest to ensure no new bugs have been introduced.<br>
5. Submit a pull request detailing your changes.

*_This README provides an overview of the project, guides on the .devcontainer usage, instructions on running tests, file structure and outlines the contribution process. Modify or expand sections as needed for your project's specific requirements._*


