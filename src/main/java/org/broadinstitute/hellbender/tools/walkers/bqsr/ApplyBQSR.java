package org.broadinstitute.hellbender.tools.walkers.bqsr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
public final class ApplyBQSR extends ReadWalker{

    private static final Logger logger = LogManager.getLogger(ApplyBQSR.class);

    @Argument(fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME, shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, doc="Write output to this file")
    public File OUTPUT;

    /**
     * Enables recalibration of base qualities.
     * The covariates tables are produced by the BaseRecalibrator tool.
     * Please be aware that you should only run recalibration with the covariates file created on the same input bam(s).
     */
    @Argument(fullName=StandardArgumentDefinitions.BQSR_TABLE_LONG_NAME, shortName=StandardArgumentDefinitions.BQSR_TABLE_SHORT_NAME, doc="Input covariates table file for base quality score recalibration")
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
        final CountingReadFilter countedFilter = disable_all_read_filters ?
                new CountingReadFilter("Allow all", ReadFilterLibrary.ALLOW_ALL_READS ) :
                makeReadFilter();
        System.out.println("disable_all_read_filters " + disable_all_read_filters);

        //pp Modify
        String MyToolName = getClass().getSimpleName();
        String str = "ApplyBQSR";
        Boolean runMyCode = true;


        class ApplyBqsrProducer implements Runnable {   //to produce the new read and transform
            private GATKReadCircularBuffer buffer;
            private Iterator<GATKRead> iterator;
            private ProgressMeter progressMeter;
            private ReadTransformer transform;

            ApplyBqsrProducer(GATKReadCircularBuffer buffer,
                              final Iterator<GATKRead> iterator,
                              ProgressMeter progressMeter,
                              ReadTransformer transform
                              ){
                this.buffer = buffer;
                this.iterator = iterator;
                this.progressMeter = progressMeter;
                this.transform = transform;

            }
             public void run(){
                 long counter = 0;

                 while(iterator.hasNext())
                 {
                     GATKRead read = iterator.next();
                     final SimpleInterval readInterval = getReadInterval(read);
                     progressMeter.update(readInterval);
                     GATKRead transformRead = transform.apply(read);

                     try{
                         buffer.put(transformRead);
                         counter++;
                     }
                     catch (InterruptedException e){
                         logger.info("producer exception");
                     }
                 }
                 try{
                     buffer.put(null);
                     counter ++;
                     logger.info("transform reads number: " + counter);
                 }catch(InterruptedException e){
                     logger.info("producer exception");
                 }

             }
        }

        class ApplyBqsrConsumer implements Runnable {
            private SAMFileGATKReadWriter outputWriter;
            private GATKReadCircularBuffer buffer;

            ApplyBqsrConsumer(
                    SAMFileGATKReadWriter outputWriter,
                    GATKReadCircularBuffer buffer
            ){
                this.outputWriter = outputWriter;
                this.buffer = buffer;
            }
            public void run(){
                long counter = 0;
                try{
                    while(true){
                        GATKRead transformRead = buffer.take();
                        counter ++;
                        if(transformRead == null) break;
                        outputWriter.addRead(transformRead);
                    }

                }catch (InterruptedException e){
                    logger.info("consumer exception");

                }
                logger.info("consumer process number: " + counter);

            }
        }

        if(MyToolName.equals(str) && runMyCode) {
            //ReadTransformer transform;
            System.out.println("Hello World start to run modified ApplyBQSR code");
            final Iterator<GATKRead> iter  = this.reads.iterator();
            //long counter = 0;

            GATKReadCircularBuffer buffer = new GATKReadCircularBuffer();
            ApplyBqsrConsumer consumer = new ApplyBqsrConsumer(outputWriter, buffer);
            ApplyBqsrProducer producer = new ApplyBqsrProducer(buffer, iter, progressMeter, transform);

            Thread threadProducer = new Thread(producer);
            Thread threadConsumer = new Thread(consumer);
            threadProducer.start();
            threadConsumer.start();
            try{
                threadProducer.join();
                threadConsumer.join();
            }catch(InterruptedException e)
            {
                logger.info("Exception in ApplyBQSR thread join");
            }
/*
            while(iter.hasNext())
            {
                //counter ++;
                GATKRead read = iter.next();
                final SimpleInterval readInterval = getReadInterval(read);
                //apply(read,
                //        new ReferenceContext(reference, readInterval), // Will create an empty ReferenceContext if reference or readInterval == null
                //        new FeatureContext(features, readInterval));
                // modify apply
                GATKRead transformRead =transform.apply(read);
                outputWriter.addRead(transformRead);


                progressMeter.update(readInterval);

            }
*/


            //System.out.println(counter);
            //System.out.println("test hasNext: " + iter.hasNext());
            logger.info(countedFilter.getSummaryLine());

        }
        else{
            //original code
            System.out.println(MyToolName+"show");
            System.out.println("original flow");
            StreamSupport.stream(reads.spliterator(), false)
                    .filter(countedFilter)
                    .forEach(read -> {
                        final SimpleInterval readInterval = getReadInterval(read);
                        apply(read,
                                new ReferenceContext(reference, readInterval), // Will create an empty ReferenceContext if reference or readInterval == null
                                new FeatureContext(features, readInterval));   // Will create an empty FeatureContext if features or readInterval == null

                        progressMeter.update(readInterval);
                    });

            logger.info(countedFilter.getSummaryLine());

        }
    }

    @Override
    public void apply( GATKRead read, ReferenceContext referenceContext, FeatureContext featureContext ) {
        //pp modify
        GATKRead transformRead = transform.apply(read);
        outputWriter.addRead(transformRead);
        //System.out.println("Test Code");
        //outputWriter.addRead(transform.apply(read));
    }

    @Override
    public void closeTool() {
        if ( outputWriter != null ) {
            outputWriter.close();
        }
    }
}
