import { createHash } from 'node:crypto';
import { once } from 'node:events';
import fs from 'node:fs';
import { PassThrough } from 'node:stream';
import type { S3Client } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import type { CsvRow } from './csv.js';
import { headers } from './csv.js';

function escapeCsvValue(value: string | number): string {
  const text = String(value ?? '');
  if (/[",\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
  return text;
}

function headerLine(): string {
  return `${headers.join(',')}\n`;
}

function rowLine(row: CsvRow): string {
  return `${headers.map((header) => escapeCsvValue(row[header])).join(',')}\n`;
}

export type SinkStats = {
  location: string;
  rows: number;
  bytes: number;
  sha256: string;
};

export interface CsvSink {
  location: string;
  append(rows: CsvRow[], writeHeaderIfNeeded?: boolean): Promise<void>;
  close(): Promise<SinkStats>;
}

abstract class BaseCsvSink implements CsvSink {
  private headerWritten = false;
  private rows = 0;
  private bytes = 0;
  private hash = createHash('sha256');
  private closed = false;

  constructor(public location: string) {}

  async append(rows: CsvRow[], writeHeaderIfNeeded = false): Promise<void> {
    this.ensureOpen();

    if (!this.headerWritten && writeHeaderIfNeeded) {
      await this.writeTrackedLine(headerLine(), true);
      this.headerWritten = true;
    }

    for (const row of rows) {
      await this.writeTrackedLine(rowLine(row), false);
    }
  }

  async close(): Promise<SinkStats> {
    this.ensureOpen();
    if (!this.closed) {
      await this.finish();
      this.closed = true;
    }

    return {
      location: this.location,
      rows: this.rows,
      bytes: this.bytes,
      sha256: this.hash.digest('hex'),
    };
  }

  protected abstract writeRaw(line: string): Promise<void>;

  protected async finish(): Promise<void> {}

  private ensureOpen(): void {
    if (this.closed) {
      throw new Error(`Sink is already closed: ${this.location}`);
    }
  }

  private async writeTrackedLine(line: string, isHeader: boolean): Promise<void> {
    await this.writeRaw(line);
    if (!isHeader) this.rows += 1;
    this.bytes += Buffer.byteLength(line, 'utf8');
    this.hash.update(line, 'utf8');
  }
}

export class LocalCsvSink extends BaseCsvSink {
  private handle?: fs.promises.FileHandle;

  protected override async writeRaw(line: string): Promise<void> {
    if (!this.handle) {
      this.handle = await fs.promises.open(this.location, 'w');
    }
    await this.handle.write(line, undefined, 'utf8');
  }

  protected override async finish(): Promise<void> {
    await this.handle?.close();
    this.handle = undefined;
  }
}

abstract class StreamingCsvSink extends BaseCsvSink {
  private stream = new PassThrough();
  private readonly uploadDone: Promise<void>;

  constructor(location: string, startUpload: (stream: PassThrough) => Promise<void>) {
    super(location);
    this.uploadDone = startUpload(this.stream);
  }

  protected override async writeRaw(line: string): Promise<void> {
    if (this.stream.write(line)) return;
    await once(this.stream, 'drain');
  }

  protected override async finish(): Promise<void> {
    this.stream.end();
    await this.uploadDone;
  }
}

export class S3CsvSink extends StreamingCsvSink {
  constructor(s3: S3Client, bucket: string, key: string, location: string) {
    super(location, async (stream) => {
      const upload = new Upload({
        client: s3,
        params: {
          Bucket: bucket,
          Key: key,
          Body: stream,
          ContentType: 'text/csv',
        },
      });

      await upload.done();
    });
  }
}
