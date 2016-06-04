package org.broadinstitute.hellbender.tools.walkers.bqsr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.SystemClock;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.ArgumentCollection;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;

import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.ReadProgramGroup;
import org.broadinstitute.hellbender.engine.*;
import org.broadinstitute.hellbender.engine.filters.CountingReadFilter;
import org.broadinstitute.hellbender.engine.filters.ReadFilterLibrary;
import org.broadinstitute.hellbender.tools.ApplyBQSRArgumentCollection;
import org.broadinstitute.hellbender.transformers.BQSRReadTransformer;
import org.broadinstitute.hellbender.transformers.ReadTransformer;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.SAMFileGATKReadWriter;
import org.mortbay.jetty.AbstractGenerator;

import java.io.File;
import java.util.Iterator;
import java.util.stream.StreamSupport;

@CommandLineProgramProperties(
        summary = "Applies the BQSR table to the input SAM/BAM/CRAM",
        oneLineSummary = "Applies the BQSR table to the input SAM/BAM/CRAM",
        programGroup = ReadProgramGroup.class
)
public final class ApplyBQSR extends ReadWalker {

    private static final Logger logger = LogManager.getLogger(ApplyBQSR.class);

    @Argument(fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME, shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, doc = "Write output to this file")
    public File OUTPUT;

    /**
     * Enables recalibration of base qualities.
     * The covariates tables are produced by the BaseRecalibrator tool.
     * Please be aware that you should only run recalibration with the covariates file created on the same input bam(s).
     */
    @Argument(fullName = StandardArgumentDefinitions.BQSR_TABLE_LONG_NAME, shortName = StandardArgumentDefinitions.BQSR_TABLE_SHORT_NAME, doc = "Input covariates table file for base quality score recalibration")
    public File BQSR_RECAL_FILE;

    /**
     * command-line arguments to fine tune the recalibration.
     */
    @ArgumentCollection
    public ApplyBQSRArgumentCollection bqsrArgs = new ApplyBQSRArgumentCollection();

    private SAMFileGATKReadWriter outputWriter;

    private ReadTransformer transform;

    @Override
    public void onTraversalStart() {
        outputWriter = createSAMWriter(OUTPUT, true);
        transform = new BQSRReadTransformer(getHeaderForReads(), BQSR_RECAL_FILE, bqsrArgs);
        Utils.warnOnNonIlluminaReadGroups(getHeaderForReads(), logger);
    }

    //pp modify
    @Override
    public void traverse() {


        class ApplyBqsrProducer implements Runnable {   //to produce the new read and transform
            private GATKReadCircularBuffer buffer;
            private Iterator<GATKRead> iterator;
            private ProgressMeter progressMeter;

            private CountingReadFilter countedFilter;

            ApplyBqsrProducer(GATKReadCircularBuffer buffer,
                              final Iterator<GATKRead> iterator,
                              ProgressMeter progressMeter,
                              CountingReadFilter countedFilter

            ) {
                this.buffer = buffer;
                this.iterator = iterator;
                this.progressMeter = progressMeter;
                this.countedFilter = countedFilter;


            }

            public void run() {
                long counter = 0;

                while (iterator.hasNext()) {
                    GATKRead read = iterator.next();
                    if(countedFilter.test(read)) {
                        final SimpleInterval readInterval = getReadInterval(read);
                        progressMeter.update(readInterval);
                        //GATKRead transformRead = transform.apply(read);

                        try {
                            //buffer.put(transformRead);
                            buffer.put(read);
                            counter++;
                        } catch (InterruptedException e) {
                            logger.info("producer exception");
                        }
                    }
                }
                try {
                    //mark the end of put
                    buffer.put(null);
                    counter++;
                    logger.info("transform reads number: " + counter);
                } catch (InterruptedException e) {
                    logger.info("producer exception");
                }

            }
        }

        class ApplyBqsrConsumer implements Runnable {
            private ReadTransformer transform;
            private SAMFileGATKReadWriter outputWriter;
            private GATKReadCircularBuffer buffer;

            ApplyBqsrConsumer(
                    ReadTransformer transform,
                    SAMFileGATKReadWriter outputWriter,
                    GATKReadCircularBuffer buffer
            ) {
                this.transform = transform;
                this.outputWriter = outputWriter;
                this.buffer = buffer;
            }

            public void run() {
                long counter = 0;
                try {
                    while (true) {
                        GATKRead takeRead = buffer.take();
                        if (takeRead == null) break;
                        GATKRead transformRead = transform.apply(takeRead);
                        //GATKRead transformRead = buffer.take();
                        counter++;

                        outputWriter.addRead(transformRead);
                    }

                } catch (InterruptedException e) {
                    logger.info("consumer exception");

                }
                logger.info("consumer process number: " + counter);

            }
        }

        final CountingReadFilter countedFilter = disable_all_read_filters ?
                new CountingReadFilter("Allow all", ReadFilterLibrary.ALLOW_ALL_READS) :
                makeReadFilter();
        System.out.println("disable_all_read_filters " + disable_all_read_filters);





        if (bqsrArgs.useProducerConsumer) {

            System.out.println("use Producer Consumer " + bqsrArgs.useProducerConsumer);
            if(bqsrArgs.useThreadPool) {
                System.out.println("use Thread Pool for transform " + bqsrArgs.useThreadPool);
            }
            else{
                System.out.println("use Thread Pool for transform " + bqsrArgs.useThreadPool);
            }

            //pp Modify
            String MyToolName = getClass().getSimpleName();
            String str = "ApplyBQSR";


            //ReadTransformer transform;
            System.out.println("Hello World start to run modified ApplyBQSR code");
            System.out.println("test sleep count, change the sleep time");
            final Iterator<GATKRead> iter = this.reads.iterator();
            //long counter = 0;

            GATKReadCircularBuffer buffer = new GATKReadCircularBuffer();
            ApplyBqsrConsumer consumer = new ApplyBqsrConsumer(transform, outputWriter, buffer);
            ApplyBqsrProducer producer = new ApplyBqsrProducer(buffer, iter, progressMeter, countedFilter);

            Thread threadProducer = new Thread(producer);
            Thread threadConsumer = new Thread(consumer);
            threadProducer.start();
            threadConsumer.start();
            try {
                threadProducer.join();
                threadConsumer.join();
            } catch (InterruptedException e) {
                logger.info("Exception in ApplyBQSR thread join");
            }

            long waitCount = buffer.countConsumer + buffer.countProducer;

            //System.out.println(counter);
            //System.out.println("test hasNext: " + iter.hasNext());
            System.out.println("wait Count Producer: " + buffer.countProducer);
            System.out.println("wait Count Consumer: " + buffer.countConsumer);
            System.out.println("wait Count Total: " + waitCount);

            logger.info(countedFilter.getSummaryLine());

        } else {
            //original code
            //System.out.println(MyToolName+" show");


            System.out.println("original flow in while form @");

            /*
            //  original code
            StreamSupport.stream(reads.spliterator(), false)
                    .filter(countedFilter)
                    .forEach(read -> {
                        final SimpleInterval readInterval = getReadInterval(read);
                        apply(read,
                                new ReferenceContext(reference, readInterval), // Will create an empty ReferenceContext if reference or readInterval == null
                                new FeatureContext(features, readInterval));   // Will create an empty FeatureContext if features or readInterval == null

                        progressMeter.update(readInterval);
                    });
            */
            ///*
            final Iterator<GATKRead> iter = this.reads.iterator();

            long timeNext = 0;
            long timeCounterFilter = 0;
            long timeTransform = 0;

            long timeAddRead = 0;
            long timeProgress = 0;

            long startNext;
            long startCounterFilter;
            long startTransform;
            long startAddRead;
            long startProgress;
            long startEnd;

            while(iter.hasNext())
            {
                //counter ++;

                startNext = System.nanoTime();

                GATKRead read = iter.next();

                startCounterFilter = System.nanoTime();



                if(countedFilter.test(read)) {

                    startTransform = System.nanoTime();

                    final SimpleInterval readInterval = getReadInterval(read);
                    GATKRead transformRead = transform.apply(read);

                    startAddRead = System.nanoTime();

                    outputWriter.addRead(transformRead);

                    startProgress = System.nanoTime();

                    progressMeter.update(readInterval);

                    startEnd = System.nanoTime();

                    //update timer
                    timeCounterFilter += startTransform - startCounterFilter;
                    timeTransform += startAddRead - startTransform;
                    timeAddRead += startProgress - startAddRead;
                    timeProgress += startEnd - startProgress;

                }

                timeNext += startCounterFilter - startNext;

            }
            //*/

            logger.info(countedFilter.getSummaryLine());

            logger.info("total timeNext " + timeNext);
            logger.info("total timeCounterFilter " + timeCounterFilter);
            logger.info("total timeTransform "+timeTransform);
            logger.info("total timeAddRead " + timeAddRead);
            logger.info("total timeProgress " + timeProgress);

        }
    }

    @Override
    public void apply(GATKRead read, ReferenceContext referenceContext, FeatureContext featureContext) {
        //pp modify
        GATKRead transformRead = transform.apply(read);
        outputWriter.addRead(transformRead);
        //System.out.println("Test Code");
        //outputWriter.addRead(transform.apply(read));
    }

    @Override
    public void closeTool() {
        if (outputWriter != null) {
            outputWriter.close();
        }
    }
}
