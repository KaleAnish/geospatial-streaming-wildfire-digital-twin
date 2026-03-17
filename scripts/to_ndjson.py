import os

def convert_to_ndjson(input_path, output_path):
    print(f"Converting {input_path} to {output_path} (NDJSON)...")
    with open(input_path, 'r', encoding='utf-8') as fin, open(output_path, 'w', encoding='utf-8') as fout:
        # We need to skip the FeatureCollection header:
        # { "type":"FeatureCollection", "features": [
        
        # Simple line-by-line processing to avoid loading everything into memory
        count = 0
        for line in fin:
            trimmed = line.strip()
            # If it looks like a feature line
            if trimmed.startswith('{"type":"Feature"'):
                # Remove trailing comma if present
                if trimmed.endswith(','):
                    trimmed = trimmed[:-1]
                # And trailing ]} for the last line
                if trimmed.endswith(']}'):
                    trimmed = trimmed[:-2]
                
                fout.write(trimmed + '\n')
                count += 1
                if count % 100000 == 0:
                    print(f"Processed {count} features...")
        
    print(f"Finished. Total features: {count}")

if __name__ == "__main__":
    convert_to_ndjson("data/California.geojson", "data/California.ndjson")
