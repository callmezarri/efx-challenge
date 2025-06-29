package com.zarr;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;

import static org.apache.beam.sdk.values.TypeDescriptors.integers;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class Challenge {
	private static final Logger log = LoggerFactory.getLogger(Challenge.class);

	public interface Options extends PipelineOptions {
		@Description("Input path for dataKeys file")
		@Default.String("")
		@Validation.Required
		String getInputPath();
		void setInputPath(String value);

		@Description("Path to start searching for data file")
		@Default.String("")
		@Validation.Required
		String getFilePath();
		void setFilePath(String value);

		@Description("Delete destination file")
		@Default.Boolean(false)
		@Validation.Required
		Boolean getDeletion();
		void setDeletion(Boolean value);

		@Description("Run naive Hashset or run with new Sets transforms")
		@Default.String("n")
		@Validation.Required
		String getMode();
		void setMode(String value);
	}


	public static Pipeline buildHashSetPipeline(Pipeline pipeline, String inputPath, String filesPath, Boolean deletion) {
		PCollectionView<Set<Integer>> dataKeys = pipeline
				.apply("Get DataKeys", TextIO.read().from(inputPath))
				.apply("Make elements integers", MapElements.into(integers()).via(Integer::parseInt))
				.apply("Make elements a Set of Integers", Combine.globally(
						new ToHashSetCombineFn<Integer>()
				))
				.apply("Make it singleton assuming it can be stored in memory", View.asSingleton());

		PCollection<Integer> data = pipeline
				.apply("Get data from csv", TextIO.read().from(filesPath))
				.apply("Make elements integers", MapElements.into(integers()).via(Integer::parseInt));

		PCollection<String> filtered = data.apply("Filter based using keys", ParDo.of(new DoFn<Integer, Integer>() {
			@ProcessElement
			public void processElement(ProcessContext c) {
				Set<Integer> keys = c.sideInput(dataKeys);
				if(!keys.contains(c.element())){
					c.output(c.element());
				}
			}
		}).withSideInputs(dataKeys)).apply("Make elements strings to write them", MapElements.into(strings()).via(String::valueOf));

		filtered.apply(TextIO.write().to(filesPath).withoutSharding().withSuffix(".filtered"));

		if(deletion){
			DeleteFileAnRenameResult(filesPath);
		}

		return pipeline;
	}

	public static Pipeline buildSetsPipeline(Pipeline pipeline, String inputPath, String filesPath, Boolean deletion) {
		PCollection<Integer> dataKeys = pipeline
				.apply("Get DataKeys", TextIO.read().from(inputPath))
				.apply(MapElements.into(integers()).via(Integer::parseInt));

		PCollection<Integer> data = pipeline
				.apply("Get data form files", TextIO.read().from(filesPath))
				.apply(MapElements.into(integers()).via(Integer::parseInt));

		PCollection<Integer> differenceAll = PCollectionList.of(data).and(dataKeys).apply("Transform using Sets transform", Sets.exceptAll());

		differenceAll.apply("Make elements strings to write them", MapElements.into(strings()).via(String::valueOf))
				.apply(TextIO.write().to(filesPath).withoutSharding().withSuffix(".filtered"));

		if(deletion){
			DeleteFileAnRenameResult(filesPath);
		}

		return pipeline;
	}

	public static Pipeline buildMultipleFile(Pipeline pipeline, String inputPath, String filesPath, Boolean deletion) {
		PCollection<FileIO.ReadableFile> destination = pipeline
				.apply("Get destination files", FileIO.match().filepattern(inputPath))
				.apply( "Store file names for deletion", FileIO.readMatches());

		PCollectionView<Set<String>> dataKeys = pipeline
				.apply("Get DataKeys", TextIO.read().from(inputPath))
				.apply("Make elements a Set of Integers", Combine.globally(
                        new ToHashSetCombineFn<>()
				))
				.apply("Make it singleton assuming it can be stored in memory", View.asSingleton());

		PCollection<KV<String, String>> data = destination.apply(ParDo.of(new DoFn<FileIO.ReadableFile, KV<String, String>>() {
			@ProcessElement
			public void processElement(ProcessContext c) throws Exception {
				FileIO.ReadableFile readableFile = c.element();
				String filename = readableFile.getMetadata().resourceId().getFilename();

				try (BufferedReader reader = new BufferedReader(new InputStreamReader(
						Channels.newInputStream(readableFile.open()), StandardCharsets.UTF_8))) {
					String line;
					while ((line = reader.readLine()) != null) {
						c.output(KV.of(line, filename));
					}
				}
			}
		}));

		PCollection<KV<String, Iterable<String>>> groupData = data.apply(GroupByKey.<String, String>create());

		PCollection<KV<String, String>> filtered = groupData
				.apply("Filter based using keys", ParDo.of(new DoFn<KV<String, Iterable<String>> , KV<String, String>>() {
			@ProcessElement
			public void processElement(ProcessContext c) {
				Set<String> keys = c.sideInput(dataKeys);
				Iterable<String> files = c.element().getValue();
				if(!keys.contains(c.element().getKey())){
					for (String filename : files) {
						c.output(KV.of(filename, c.element().getKey()));
					}
				}
			}
		}).withSideInputs(dataKeys));

		//TODO
		//Write on files based on keys
		//Delete and replace logic for multiple files

		return pipeline;
	}

	public static void DeleteFileAnRenameResult(String path){
		ResourceId toDelete = FileSystems.matchNewResource(path, false);
		ResourceId toRename = FileSystems.matchNewResource(path, false);
		ResourceId newName = FileSystems.matchNewResource(path.substring(0, path.length() - 9), false);

		try {
			FileSystems.delete(Collections.singletonList(toDelete));
			FileSystems.rename(Collections.singletonList(toRename), Collections.singletonList(newName));
		} catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

	public static void main(String[] args) {

		var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		var pipeline = Pipeline.create(options);

		if("n".equals(options.getMode())) {
			Challenge.buildHashSetPipeline(pipeline, options.getInputPath(), options.getFilePath(), options.getDeletion());
			pipeline.run().waitUntilFinish();
		}else {
			Challenge.buildSetsPipeline(pipeline, options.getInputPath(), options.getFilePath(), options.getDeletion());
			pipeline.run().waitUntilFinish();
		}

	}
}