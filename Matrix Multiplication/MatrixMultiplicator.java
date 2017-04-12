package ru.mipt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Collections;
import java.util.Comparator;

import java.lang.StringBuilder;
import java.lang.Math;


public class MatrixMultiplicator extends Configured implements Tool {
	
	// Тройка из номера строки, номера столбца и значения в матрице
	public static class Cell implements WritableComparable<Cell> {
		public LongWritable lineId;
		public LongWritable colId;
		public LongWritable value;
		
		Cell() {
			lineId = new LongWritable();
			colId = new LongWritable();
			value = new LongWritable();
		}
		
		Cell(LongWritable _lineId, LongWritable _colId, LongWritable _value) {
			lineId = new LongWritable(_lineId.get());
			colId = new LongWritable(_colId.get());
			value = new LongWritable(_value.get());
		}
		
		Cell(Cell _cell) {
			lineId = new LongWritable(_cell.lineId.get());
			colId = new LongWritable(_cell.colId.get());
			value = new LongWritable(_cell.value.get());
		}
		
		public void write(DataOutput out) throws IOException {
			lineId.write(out);
			colId.write(out);
			value.write(out);
		}
		
		public void readFields(DataInput in) throws IOException {
			lineId.readFields(in);
			colId.readFields(in);
			value.readFields(in);
		}
		
		public int compareTo(Cell o) {
			if (lineId.compareTo(o.lineId) == 0) {
				if (colId.compareTo(o.colId) == 0)
					return value.compareTo(o.value);
				return colId.compareTo(o.colId);
			}
			return lineId.compareTo(o.lineId);
		}
		
		public int hashCode() {
			return lineId.hashCode() * 1000000009 + colId.hashCode();
		}
		
	   @Override
		public String toString() {
			return "lineId = " + lineId + ", colId = " + colId + ", value = " + value; 
		}
	}
	
	
	public static class LineOrColumn implements Writable {
		public LongWritable matrixId;
		public LongWritable lineOrColId;
		public LongWritable[] data;		
		
		LineOrColumn() {
			matrixId = new LongWritable();
			lineOrColId = new LongWritable();
			data = new LongWritable[0];
		}
		
		LineOrColumn(LongWritable _matrixId, LongWritable _lineOrColId, LongWritable[] _data) {
			matrixId = new LongWritable(_matrixId.get());
			lineOrColId = new LongWritable(_lineOrColId.get());
			data = (LongWritable[])_data.clone();
		}
		
		LineOrColumn(LineOrColumn _lineOrColumn) {
			matrixId = new LongWritable(_lineOrColumn.matrixId.get());
			lineOrColId = new LongWritable(_lineOrColumn.lineOrColId.get());
			data = (LongWritable[])_lineOrColumn.data.clone();
		}
		
		public void write(DataOutput out) throws IOException {
			matrixId.write(out);
			lineOrColId.write(out);
			new LongWritable(data.length).write(out);
			for (LongWritable point : data) {
				point.write(out);
			}
		}
		
		public void readFields(DataInput in) throws IOException {
			matrixId.readFields(in);
			lineOrColId.readFields(in);
			LongWritable length = new LongWritable();
			length.readFields(in);
			data = new LongWritable[(int)length.get()];
			for (int i = 0; i < length.get(); i += 1) {
				LongWritable point = new LongWritable();
				point.readFields(in);
				data[i] = point;
			}
		}
		
	   @Override
		public String toString() {
			StringBuilder str = new StringBuilder("matrixId = " + matrixId + ", lineOrColId = " + lineOrColId + ", data: ");
			for (LongWritable point : data) {
				str.append(point.get());
				str.append(" ");
			}
			return str.toString();
		}
	}
	
	
	public static class StringToCellMapper extends Mapper<LongWritable, Text, LongWritable, Cell> {
		@Override
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
			String str = line.toString();
			StringTokenizer tokenizer = new StringTokenizer(str, ",");
			LongWritable lineId = new LongWritable(new Long(tokenizer.nextToken()));
			LongWritable colId = new LongWritable(new Long(tokenizer.nextToken()));
			LongWritable value = new LongWritable(new Long(tokenizer.nextToken()));
			
			ContextWrite(context, new Cell(lineId, colId, value));
		}
		
		public void ContextWrite(Context context, Cell cell) throws IOException, InterruptedException {}
	}
	
	public static class StringToCellForAMapper extends StringToCellMapper {
		@Override
		public void ContextWrite(Context context, Cell cell) throws IOException, InterruptedException {
			context.write(cell.lineId, cell);
		}
	}
	
	public static class StringToCellForBMapper extends StringToCellMapper {
		@Override
		public void ContextWrite(Context context, Cell cell) throws IOException, InterruptedException {
			context.write(cell.colId, cell);
		}
	}
	
	
	public static class CellToLineOrColumnReducer extends Reducer<LongWritable, Cell, LongWritable, LineOrColumn> {
		@Override
		public void reduce(LongWritable key, Iterable<Cell> cells, Context context) throws IOException, InterruptedException {			
			ArrayList<Cell> listOfCells = new ArrayList<Cell>();
			for (Cell cell : cells) {
				listOfCells.add(new Cell(cell));
			}
			
			LongWritable[] lineOrColumnData = new LongWritable[listOfCells.size()];
			for (Cell cell : listOfCells) {
				if (GetMatrixId() == -1) { // тогда key - номер строчки
					lineOrColumnData[(int)cell.colId.get()] = cell.value;
				}
				else { // key - номер столбца
					lineOrColumnData[(int)cell.lineId.get()] = cell.value;
				}
			}			
			
			context.write(new LongWritable(GetMatrixId() * (key.get() + 1)),
						  new LineOrColumn(new LongWritable(GetMatrixId()), key, lineOrColumnData));
		}
		
		public int GetMatrixId() { // A -> -1, B -> 1
			return 0;
		} 
	}
	
	// для матрицы A
	public static class CellToLineReducer extends CellToLineOrColumnReducer {
		@Override
		public int GetMatrixId() {
			return -1;
		}
	}
	
	// для матрицы B
	public static class CellToColumnReducer extends CellToLineOrColumnReducer {
		@Override
		public int GetMatrixId() {
			return 1;
		}
	}
	
	public static class LineOrColumnSendMapper extends Mapper<LongWritable, LineOrColumn, LongWritable, LineOrColumn> {
		@Override
		public void map(LongWritable key, LineOrColumn lineOrColumn, Context context) throws IOException, InterruptedException {
			// Получим количество reduce tasks
			Integer numReduceTasks = context.getNumReduceTasks();
			
			// Для строчек, соответствующих матрице A просто переотправим их дальше
			if (key.get() < 0) {
				context.write(key, lineOrColumn);
			}
			// Для строчек соответствующих матрице B, отправим их numReduceTasks раз
			else {
				for (int i = 0; i < numReduceTasks; i += 1) {
					context.write(new LongWritable(i), lineOrColumn); // i - reduce task, на который должна будет прийти запись
				}
			}
		}
	}
	
	public static class PartitionerForLinesAndColumns extends Partitioner<LongWritable, LineOrColumn> {
		@Override
		public int getPartition(LongWritable key, LineOrColumn lineOrColumn, int numReduceTasks) {
			if (key.get() < 0) {
				return Math.abs(key.hashCode()) % numReduceTasks;
			}
			return (int)key.get();
		}
	}
	
	
	public static class MultiplicatorOfLinesAndColumnsReducer extends Reducer<LongWritable, LineOrColumn, Text, Cell> {
		// Список, в котором будем сохранять принятые строчки матрицы A
		private ArrayList<LineOrColumn> listOfLinesOfMatrixA = new ArrayList<LineOrColumn>();
		
		@Override
		public void reduce(LongWritable key, Iterable<LineOrColumn> linesOrColumns, Context context) throws IOException, InterruptedException {
			if (key.get() < 0) {
				for (LineOrColumn loc : linesOrColumns) {
					// запомним строчку
					listOfLinesOfMatrixA.add(new LineOrColumn(loc));
				}
			}
			else {
				for (LineOrColumn col : linesOrColumns) {
					LineOrColumn currentColumn = new LineOrColumn(col);
					
					for (LineOrColumn line : listOfLinesOfMatrixA) {
						
						if (currentColumn.data.length != line.data.length)
							throw new IOException("NOT EQUAL LENGTH");
						
						long sum = 0;
						for (int i = 0; i < line.data.length; i += 1) {
							sum += line.data[i].get() * currentColumn.data[i].get();
						}
						
						Cell resultCell = new Cell(line.lineOrColId, 
												   currentColumn.lineOrColId,
												   new LongWritable(sum));
						context.write(new Text("result"), resultCell);
					}
				}
			}
		}
	}

	private static void deleteFolder(FileSystem fs, Path... paths) throws IOException {
		for (Path path: paths) {
			if (fs.exists(path)) {
				fs.deleteOnExit(path);
			}
		}
	}
	
	private void runMatrixTransformer(int matrixId, Path inputPath, Path midPath) throws Exception { // matrixId: A -> -1, B -> 1
		Configuration conf = this.getConf();
		FileSystem fs = FileSystem.get(conf);
		
		Job matrixTransformer = Job.getInstance(conf);
		matrixTransformer.setJobName("matrixTransformer");
		matrixTransformer.setJarByClass(MatrixMultiplicator.class);

		matrixTransformer.setInputFormatClass(TextInputFormat.class);
		matrixTransformer.setOutputFormatClass(SequenceFileOutputFormat.class);

		matrixTransformer.setMapperClass(matrixId == -1 ? StringToCellForAMapper.class : StringToCellForBMapper.class);
		matrixTransformer.setReducerClass(matrixId == -1 ? CellToLineReducer.class : CellToColumnReducer.class);

		matrixTransformer.setMapOutputKeyClass(LongWritable.class);
		matrixTransformer.setMapOutputValueClass(Cell.class);
		matrixTransformer.setOutputKeyClass(LongWritable.class);
		matrixTransformer.setOutputValueClass(LineOrColumn.class);

		FileInputFormat.addInputPath(matrixTransformer, inputPath);
		SequenceFileOutputFormat.setOutputPath(matrixTransformer, midPath);

		if (!matrixTransformer.waitForCompletion(true)) {
			deleteFolder(fs, midPath);
			throw new IOException("Can't complete matrixTransformer");
		}

	}

	@Override
	public int run(String[] strings) throws Exception {
		Path inputPathA = new Path(strings[0]);
		Path inputPathB = new Path(strings[1]);
		Path outputPath = new Path(strings[2]);
		Path midPathA = new Path(strings[2] + "_tmpA");
		Path midPathB = new Path(strings[2] + "_tmpB");
		Configuration conf = this.getConf();
		FileSystem fs = FileSystem.get(conf);

		deleteFolder(fs, midPathA, midPathB, outputPath);
		
		runMatrixTransformer(-1, inputPathA, midPathA);
		runMatrixTransformer(1, inputPathB, midPathB);

		Path partPath = new Path(strings[2] + "_part");

		Job multiplicator = Job.getInstance(conf);
		multiplicator.setJobName("MatrixMultiplicator");
		multiplicator.setJarByClass(MatrixMultiplicator.class);

		multiplicator.setMapperClass(LineOrColumnSendMapper.class);
		multiplicator.setPartitionerClass(PartitionerForLinesAndColumns.class);
		multiplicator.setReducerClass(MultiplicatorOfLinesAndColumnsReducer.class);

		multiplicator.setInputFormatClass(SequenceFileInputFormat.class);
		multiplicator.setOutputFormatClass(TextOutputFormat.class);

		multiplicator.setMapOutputKeyClass(LongWritable.class);
		multiplicator.setMapOutputValueClass(LineOrColumn.class);
		multiplicator.setOutputKeyClass(Text.class);
		multiplicator.setOutputValueClass(Cell.class);

		SequenceFileInputFormat.setInputPaths(multiplicator, midPathA, midPathB);
		FileOutputFormat.setOutputPath(multiplicator, outputPath);

		int resultCode = 0;
		if (!multiplicator.waitForCompletion(true)) {
			resultCode = -2;
		}
		deleteFolder(fs, midPathA, midPathB, partPath);
		return resultCode;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new MatrixMultiplicator(), args);
	}
}

