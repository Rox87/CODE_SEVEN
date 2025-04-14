from IPython.display import Image
import plantuml
import requests
from PIL import Image as PilImage
from io import BytesIO
# Create a PlantUML server instance
plantuml_server = plantuml.PlantUML(url='http://www.plantuml.com/plantuml/img/')

def generate_diagram(puml):
    # Your PlantUML code
    plantuml_code = open(puml, "r", encoding="utf-8").read()

    # Generate and display the diagram
    diagram = plantuml_server.get_url(plantuml_code)
    return Image(url=diagram)