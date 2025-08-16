import struct
import math
import logging

# Configure logging to write to a file
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='s3m_converter.log',
    filemode='w'
)

class S3MParser:
    def __init__(self, filename):
        self.filename = filename
        logging.debug(f"Initializing S3MParser with file: {filename}")
        self.title = ""
        self.orders = []
        self.instruments = []
        self.patterns = []
        self.num_orders = 0
        self.num_instruments = 0
        self.num_patterns = 0
        self.sample_rate = 44100
        self.channels = 32
        self.tempo = 125
        self.speed = 6
        
    def read_s3m(self):
        logging.debug(f"Reading S3M file: {self.filename}")
        try:
            with open(self.filename, 'rb') as f:
                data = f.read()
            logging.debug(f"Read {len(data)} bytes from {self.filename}")
        except FileNotFoundError:
            logging.error(f"File {self.filename} not found")
            raise Exception(f"File {self.filename} not found")
        except Exception as e:
            logging.error(f"Error reading file {self.filename}: {str(e)}")
            raise Exception(f"Error reading file {self.filename}: {str(e)}")

        if len(data) < 96:
            logging.error("File too small to be a valid S3M")
            raise Exception("File too small to be a valid S3M")

        # Parse header
        self.title = data[:28].decode('ascii', errors='ignore').rstrip('\0')
        logging.debug(f"Parsed title: {self.title}")
        try:
            (self.num_orders, self.num_instruments, self.num_patterns) = struct.unpack('<HHH', data[28:34])
            logging.debug(f"Header - Orders: {self.num_orders}, Instruments: {self.num_instruments}, Patterns: {self.num_patterns}")
        except struct.error:
            logging.error("Invalid S3M header")
            raise Exception("Invalid S3M header")
        flags = struct.unpack('<H', data[38:40])[0]
        logging.debug(f"Header - Flags: {flags}")
        
        # Read order list
        order_start = 96
        if order_start + self.num_orders > len(data):
            logging.error("Order list exceeds file size")
            raise Exception("Order list exceeds file size")
        self.orders = list(data[order_start:order_start + self.num_orders])
        logging.debug(f"Orders: {self.orders}")
        
        # Read instrument and pattern pointers
        inst_ptr_start = order_start + self.num_orders
        pattern_ptr_start = inst_ptr_start + 2 * self.num_instruments
        if pattern_ptr_start + 2 * self.num_patterns > len(data):
            logging.error("Pattern pointers exceed file size")
            raise Exception("Pattern pointers exceed file size")
        inst_ptrs = struct.unpack(f'<{self.num_instruments}H', data[inst_ptr_start:inst_ptr_start + 2 * self.num_instruments])
        pattern_ptrs = struct.unpack(f'<{self.num_patterns}H', data[pattern_ptr_start:pattern_ptr_start + 2 * self.num_patterns])
        logging.debug(f"Instrument pointers: {inst_ptrs}")
        logging.debug(f"Pattern pointers: {pattern_ptrs}")
        
        # Parse instruments
        for idx, ptr in enumerate(inst_ptrs):
            ptr = ptr * 16  # Paragraphs to bytes
            logging.debug(f"Parsing instrument {idx} at pointer {ptr}")
            if ptr + 24 > len(data):
                logging.warning(f"Instrument pointer {ptr} exceeds file size")
                continue
            inst_type = data[ptr]
            if inst_type == 1:  # Sample-based instrument
                inst_name = data[ptr + 1:ptr + 13].decode('ascii', errors='ignore').rstrip('\0')
                try:
                    sample_ptr, sample_len, loop_begin, loop_end, volume = struct.unpack('<LHHHB', data[ptr + 13:ptr + 24])
                    sample_ptr = sample_ptr * 16
                    logging.debug(f"Instrument {inst_name}: sample_ptr={sample_ptr}, len={sample_len}, loop_begin={loop_begin}, loop_end={loop_end}, volume={volume}")
                    if sample_ptr + sample_len > len(data):
                        logging.warning(f"Sample data for instrument {inst_name} exceeds file size")
                        continue
                    sample_data = data[sample_ptr:sample_ptr + sample_len]
                    # Convert 8-bit signed to unsigned
                    sample_data = bytes((b + 128) & 0xFF for b in sample_data)
                    self.instruments.append({
                        'name': inst_name,
                        'sample': sample_data,
                        'length': sample_len,
                        'loop_begin': loop_begin,
                        'loop_end': loop_end,
                        'volume': volume / 64.0
                    })
                    logging.debug(f"Added instrument: {inst_name}")
                except struct.error:
                    logging.warning(f"Failed to parse instrument at pointer {ptr}")
        
        # Parse patterns
        for idx, ptr in enumerate(pattern_ptrs):
            ptr = ptr * 16
            logging.debug(f"Parsing pattern {idx} at pointer {ptr}")
            if ptr + 2 > len(data):
                logging.warning(f"Pattern pointer {ptr} exceeds file size")
                continue
            try:
                packed_len = struct.unpack('<H', data[ptr:ptr + 2])[0]
                logging.debug(f"Pattern {idx} packed length: {packed_len}")
                if ptr + 2 + packed_len > len(data):
                    logging.warning(f"Pattern data at {ptr} exceeds file size")
                    continue
                pattern_data = data[ptr + 2:ptr + 2 + packed_len]
                rows = []
                i = 0
                for row_idx in range(64):  # 64 rows per pattern
                    row = [None] * self.channels
                    logging.debug(f"Parsing row {row_idx} in pattern {idx}")
                    while i < len(pattern_data):
                        if pattern_data[i] == 0:  # End of row
                            i += 1
                            logging.debug(f"End of row {row_idx} at index {i}")
                            break
                        if i + 1 > len(pattern_data):
                            logging.warning(f"Incomplete pattern data at row {row_idx}, index {i}")
                            break
                        what = pattern_data[i]
                        i += 1
                        channel = what & 31
                        note = None
                        instrument = None
                        volume = None
                        effect = None
                        if what & 32:  # Note and instrument
                            if i + 2 > len(pattern_data):
                                logging.warning(f"Incomplete note/instrument data at row {row_idx}, index {i}")
                                break
                            note = pattern_data[i]
                            instrument = pattern_data[i + 1]
                            i += 2
                            logging.debug(f"Channel {channel}: note={note}, instrument={instrument}")
                        if what & 64:  # Volume
                            if i + 1 > len(pattern_data):
                                logging.warning(f"Incomplete volume data at row {row_idx}, index {i}")
                                break
                            volume = pattern_data[i]
                            i += 1
                            logging.debug(f"Channel {channel}: volume={volume}")
                        if what & 128:  # Effect
                            if i + 2 > len(pattern_data):
                                logging.warning(f"Incomplete effect data at row {row_idx}, index {i}")
                                break
                            effect = (pattern_data[i], pattern_data[i + 1])
                            i += 2
                            logging.debug(f"Channel {channel}: effect={effect}")
                        row[channel] = {'note': note, 'instrument': instrument, 'volume': volume, 'effect': effect}
                    rows.append(row)
                self.patterns.append(rows)
                logging.debug(f"Parsed pattern {idx} with {len(rows)} rows")
            except struct.error:
                logging.warning(f"Failed to parse pattern at pointer {ptr}")

class S3MRenderer:
    def __init__(self, parser):
        self.parser = parser
        self.sample_rate = parser.sample_rate
        self.channel_states = [{'sample_pos': 0, 'increment': 0, 'volume': 0, 'playing': False, 'instrument': None} for _ in range(parser.channels)]
        logging.debug(f"Initialized renderer with {parser.channels} channels, sample rate {self.sample_rate}")
        
    def note_to_freq(self, note, c2spd=8363):
        if note is None or note == 254:  # 254 = key off
            logging.debug(f"Note is None or key off: {note}")
            return 0
        octave = note >> 4
        note = note & 0x0F
        # S3M note table (Amiga frequencies)
        note_table = [261.63, 277.18, 293.66, 311.13, 329.63, 349.23, 369.99, 392.00, 415.30, 440.00, 466.16, 493.88]
        if note >= len(note_table):
            logging.warning(f"Invalid note index: {note}")
            return 0
        freq = note_table[note] * (2 ** (octave - 4))
        result = freq * c2spd / 8363
        logging.debug(f"Converted note {note} (octave {octave}) to frequency: {result} Hz")
        return result

    def render(self):
        logging.debug("Starting audio rendering")
        output = []
        ticks_per_row = self.parser.speed
        samples_per_tick = self.parser.sample_rate // (self.parser.tempo * 4 // 60)
        logging.debug(f"Rendering parameters: ticks_per_row={ticks_per_row}, samples_per_tick={samples_per_tick}")
        
        for order_idx, order in enumerate(self.parser.orders):
            if order >= len(self.parser.patterns):
                logging.warning(f"Invalid pattern order {order} at order index {order_idx}")
                continue
            pattern = self.parser.patterns[order]
            logging.debug(f"Rendering pattern {order} (order index {order_idx})")
            for row_idx, row in enumerate(pattern):
                logging.debug(f"Processing row {row_idx} in pattern {order}")
                # Process row
                for ch, event in enumerate(row):
                    if event and event['note'] is not None:
                        logging.debug(f"Channel {ch}: event={event}")
                        if event['note'] == 254:  # Key off
                            self.channel_states[ch]['playing'] = False
                            logging.debug(f"Channel {ch}: Key off")
                        else:
                            inst_idx = event['instrument'] - 1 if event['instrument'] else None
                            if inst_idx is not None and inst_idx < len(self.parser.instruments):
                                self.channel_states[ch]['instrument'] = self.parser.instruments[inst_idx]
                                self.channel_states[ch]['sample_pos'] = 0
                                self.channel_states[ch]['increment'] = self.note_to_freq(event['note']) / self.sample_rate
                                self.channel_states[ch]['volume'] = event['volume'] / 64.0 if event['volume'] is not None else self.parser.instruments[inst_idx]['volume']
                                self.channel_states[ch]['playing'] = True
                                logging.debug(f"Channel {ch}: Playing instrument {inst_idx}, note={event['note']}, volume={self.channel_states[ch]['volume']}")
                
                # Render audio for this row
                for _ in range(ticks_per_row * samples_per_tick):
                    sample = 0
                    for ch in range(self.parser.channels):
                        state = self.channel_states[ch]
                        if state['playing'] and state['instrument']:
                            pos = int(state['sample_pos'])
                            inst = state['instrument']
                            if pos >= inst['length']:
                                if inst['loop_end'] > inst['loop_begin']:
                                    pos = inst['loop_begin'] + (pos - inst['loop_end']) % (inst['loop_end'] - inst['loop_begin'])
                                else:
                                    state['playing'] = False
                                    logging.debug(f"Channel {ch}: Stopped playing (sample pos {pos} >= length {inst['length']})")
                                    continue
                            sample += (inst['sample'][pos] - 128) * state['volume']
                            state['sample_pos'] += state['increment']
                    sample = max(min(int(sample * 128 + 128), 255), 0)  # 8-bit unsigned
                    output.append(sample)
        logging.debug(f"Rendering complete. Generated {len(output)} samples")
        return bytes(output)

class WAVWriter:
    def __init__(self, filename, sample_rate=44100):
        self.filename = filename
        self.sample_rate = sample_rate
        logging.debug(f"Initializing WAVWriter for file: {filename}, sample rate: {sample_rate}")
        
    def write(self, audio_data):
        logging.debug(f"Writing WAV file: {self.filename}")
        num_samples = len(audio_data)
        logging.debug(f"Audio data contains {num_samples} samples")
        # WAV header
        header = bytearray()
        header.extend(b'RIFF')
        header.extend(struct.pack('<L', 36 + num_samples))  # Chunk size
        header.extend(b'WAVE')
        header.extend(b'fmt ')
        header.extend(struct.pack('<LHHLLHH', 16, 1, 1, self.sample_rate, self.sample_rate, 1, 8))  # PCM format
        header.extend(b'data')
        header.extend(struct.pack('<L', num_samples))
        
        try:
            with open(self.filename, 'wb') as f:
                f.write(header)
                f.write(audio_data)
            logging.debug(f"Successfully wrote WAV file: {self.filename}")
        except Exception as e:
            logging.error(f"Error writing WAV file {self.filename}: {str(e)}")
            raise Exception(f"Error writing WAV file {self.filename}: {str(e)}")

def convert_s3m_to_wav(s3m_file, wav_file):
    logging.debug(f"Starting conversion: {s3m_file} -> {wav_file}")
    parser = S3MParser(s3m_file)
    parser.read_s3m()
    renderer = S3MRenderer(parser)
    audio_data = renderer.render()
    writer = WAVWriter(wav_file)
    writer.write(audio_data)
    logging.debug(f"Conversion completed: {wav_file}")

# Example usage
if __name__ == "__main__":
    try:
        convert_s3m_to_wav("2ND_PM.S3M", "2ND_PM.WAV")
        print("Conversion completed. Check s3m_converter.log for details.")
    except Exception as e:
        print(f"Error during conversion: {str(e)}. Check s3m_converter.log for details.")