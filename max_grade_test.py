# Map/Reduce task example
# Finding max grade of tests for each person


# Map function for max grade task
def max_grade_map(data, first, last):
    mapped_data = []
    for line in data[first:last]:
        max_grade = [float(line[i].replace(" ", "")) for i in ['Test1', 'Test2', 'Test3', 'Test4']]
        for i in max_grade:
            mapped_data.append([line['First name'], i])

    return mapped_data


# Reduce function for max grade task
def max_grade_reduce(data):
    reduced_data = {}
    for key, value in data:
        if key not in reduced_data:
            reduced_data[key] = float(value)
        elif reduced_data[key] < float(value):
            reduced_data[key] = float(value)

    return reduced_data.items()
