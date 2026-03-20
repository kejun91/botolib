def chunk_dict(data, max_items_per_chunk):
    chunks = []
    current_chunk = {}
    current_count = 0

    for key, values in data.items():
        for i in range(0, len(values), max_items_per_chunk - current_count):
            chunk_values = values[i:i + (max_items_per_chunk - current_count)]
            if current_chunk.get(key):
                current_chunk[key].extend(chunk_values)
            else:
                current_chunk[key] = chunk_values

            current_count += len(chunk_values)

            if current_count >= max_items_per_chunk:
                chunks.append(current_chunk)
                current_chunk = {}
                current_count = 0

    if current_chunk:
        chunks.append(current_chunk)
    
    return chunks