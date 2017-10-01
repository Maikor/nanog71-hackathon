import yaml

# Read YAML file
with open("business_logic.yaml", 'r') as stream:
    data_loaded = yaml.load(stream)

print(data_loaded)