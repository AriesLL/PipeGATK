package org.broadinstitute.hellbender.engine;

import org.broadinstitute.hellbender.engine.filters.WellformedReadFilter;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.engine.filters.CountingReadFilter;
import org.broadinstitute.hellbender.engine.filters.ReadFilterLibrary;
import org.broadinstitute.hellbender.utils.read.GATKRead;


import java.util.stream.StreamSupport;

//pp modify
import org.broadinstitute.hellbender.transformers.ReadTransformer;
import java.util.Iterator;
/**
 * A ReadWalker is a tool that processes a single read at a time from one or multiple sources of reads, with
 * optional contextual information from a reference and/or sets of variants/Features.
 *
 * If multiple sources of reads are specified, they are merged together into a single sorted stream of reads.
 *
 * ReadWalker authors must implement the apply() method to process each read, and may optionally implement
 * onTraversalStart() and/or onTraversalSuccess(). See the PrintReadsWithReference walker for an example.
 */
public abstract class ReadWalker extends GATKTool {

    @Argument(fullName = "disable_all_read_filters", shortName = "f", doc = "Disable all read filters", common = false, optional = true)
    public boolean disable_all_read_filters = false;

    @Override
    public boolean requiresReads() {
        return true;
    }

    /**
     * This number controls the size of the cache for our FeatureInputs
     * (specifically, the number of additional bases worth of overlapping records to cache when querying feature sources).
     */
    public static final int FEATURE_CACHE_LOOKAHEAD = 1_000;

    /**
     * Initialize data sources for traversal.
     *
     * Marked final so that tool authors don't override it. Tool authors should override onTraversalStart() instead.
     */
    @Override
    protected final void onStartup() {
        super.onStartup();

        if ( hasIntervals() ) {
            reads.setTraversalBounds(intervalArgumentCollection.getTraversalParameters(getHeaderForReads().getSequenceDictionary()));
        }
    }

    @Override
    void initializeFeatures() {
        //We override this method to change lookahead of the cache
        features = new FeatureManager(this, FEATURE_CACHE_LOOKAHEAD);
        if ( features.isEmpty() ) {  // No available sources of Features discovered for this tool
            features = null;
        }
    }

    /**
     * Implementation of read-based traversal.
     * Subclasses can override to provide own behavior but default implementation should be suitable for most uses.
     *
     * The default implementation creates filters using {@link #makeReadFilter}
     * and then iterates over all reads, applies the filter and hands the resulting reads to the {@link #apply}
     * function of the walker (along with additional contextual information, if present, such as reference bases).
     */
    @Override
    public void traverse() {
        // Process each read in the input stream.
        // Supply reference bases spanning each read, if a reference is available.
        final CountingReadFilter countedFilter = disable_all_read_filters ?
                                                    new CountingReadFilter("Allow all", ReadFilterLibrary.ALLOW_ALL_READS ) :
                                                    makeReadFilter();
        System.out.println("disable_all_read_filters " + disable_all_read_filters);

        //pp Modify




        /*if(MyToolName.equals(str) && runMyCode) {
            //ReadTransformer transform;
            System.out.println("start to run modified ApplyBQSR code");
            Iterator<GATKRead> iter  = reads.iterator();
            long counter = 0;
            while(iter.hasNext())
            {
                counter ++;
                GATKRead read = iter.next();
                final SimpleInterval readInterval = getReadInterval(read);
                apply(read,
                        new ReferenceContext(reference, readInterval), // Will create an empty ReferenceContext if reference or readInterval == null
                        new FeatureContext(features, readInterval));
                //
                //GATKRead transformRead =transform.apply(read);
                //outputWriter.addRead(transformRead);
                progressMeter.update(readInterval);

            }
            System.out.println(counter);
            //System.out.println("test hasNext: " + iter.hasNext());
            logger.info(countedFilter.getSummaryLine());

        }*/
//        else{
            //original code
            //System.out.println(MyToolName+"show");
            //System.out.println("original flow");
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

//        }

        //StreamSupport.stream(reads.spliterator(), false)

    }

    /**
     * Returns an interval for the read.
     * Note: some walkers must be able to work on any read, including those whose coordinates do not form a valid SimpleInterval.
     * So here we check this condition and create null intervals for such reads.
     */
    protected SimpleInterval getReadInterval(final GATKRead read) {
        return !read.isUnmapped() && SimpleInterval.isValid(read.getContig(), read.getStart(), read.getEnd()) ? new SimpleInterval(read) : null;
    }

    /**
     * Returns the read filter (simple or composite) that will be applied to the reads before calling {@link #apply}.
     * The default implementation uses the {@link WellformedReadFilter} filter with all default options.
     * Default implementation of {@link #traverse()} calls this method once before iterating
     * over the reads and reuses the filter object to avoid object allocation. Nevertheless, keeping state in filter objects is strongly discouraged.
     *
     * Subclasses can extend to provide own filters (ie override and call super).
     * Multiple filters can be composed by using {@link org.broadinstitute.hellbender.engine.filters.ReadFilter} composition methods.
     */
    public CountingReadFilter makeReadFilter(){
          return new CountingReadFilter("Wellformed", new WellformedReadFilter(getHeaderForReads()));
    }

    /**
     * Process an individual read (with optional contextual information). Must be implemented by tool authors.
     * In general, tool authors should simply stream their output from apply(), and maintain as little internal state
     * as possible.
     *
     * TODO: Determine whether and to what degree the GATK engine should provide a reduce operation
     * TODO: to complement this operation. At a minimum, we should make apply() return a value to
     * TODO: discourage statefulness in walkers, but how this value should be handled is TBD.
     * @param read current read
     * @param referenceContext Reference bases spanning the current read. Will be an empty, but non-null, context object
     *                         if there is no backing source of reference data (in which case all queries on it will return
     *                         an empty array/iterator). Can request extra bases of context around the current read's interval
     *                         by invoking {@link ReferenceContext#setWindow} on this object before calling {@link ReferenceContext#getBases}
     * @param featureContext Features spanning the current read. Will be an empty, but non-null, context object
     *                       if there is no backing source of Feature data (in which case all queries on it will return an
     *                       empty List).
     */
    public abstract void apply( GATKRead read, ReferenceContext referenceContext, FeatureContext featureContext );

    /**
     * Shutdown data sources.
     *
     * Marked final so that tool authors don't override it. Tool authors should override onTraversalSuccess() instead.
     */
    @Override
    protected final void onShutdown() {
        // Overridden only to make final so that concrete tool implementations don't override
        super.onShutdown();
    }
}
