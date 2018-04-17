# Map/Reduce task example
# Count number of each word in file


# Clean words from punctuation
def clean_word(word):
    word = ''.join(x.lower() for x in word if x.isalnum())
    return word


# Map function for words counting task
def words_count_map(data, first, last):
    mapped_data = []
    for line in data[first:last]:
        words = line.split()
        for word in words:
            mapped_data.append([clean_word(word), 1])

    return mapped_data


# Reduce function for words counting task
def words_count_reduce(data):
    reduced_data = {}
    for key, value in data:
        if key not in reduced_data:
            reduced_data[key] = int(value)
        else:
            reduced_data[key] += int(value)

    return reduced_data.items()