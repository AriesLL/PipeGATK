package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.genotyper.MostLikelyAllele;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;
import org.broadinstitute.hellbender.utils.variant.GATKVCFHeaderLines;

import java.util.Collections;
import java.util.List;
import java.util.OptionalDouble;

/**
 * Rank Sum Test of per-read likelihoods of REF versus ALT reads
 *
 * <p>This variant-level annotation compares the likelihoods of reads to their best haplotype match, between reads that support the reference allele and those that support the alternate allele. The ideal result is a value close to zero, which indicates there is little to no difference.  A negative value indicates that the reads supporting the alternate allele have lower likelihoods to their best haplotype match than those supporting the reference allele. Conversely, a positive value indicates that the reads supporting the alternate allele have higher likelihoods to their best haplotype match than those supporting the reference allele. Finding a statistically significant difference either way suggests that the sequencing and/or mapping process may have been biased or affected by an artifact.</p>
 *
 * <h3>Statistical notes</h3>
 * <p>The value output for this annotation is the u-based z-approximation from the Mann-Whitney-Wilcoxon Rank Sum Test for per-read likelihoods to the best haplotype match (likelihoods of reads supporting REF vs. likelihoods of reads supporting ALT). See the <a href="http://www.broadinstitute.org/gatk/guide/article?id=4732">method document on statistical tests</a> for a more detailed explanation of the ranksum test.</p>
 *
 * <h3>Caveat</h3>
 * <p>The read position rank sum test can not be calculated for sites without a mixture of reads showing both the reference and alternate alleles.</p>
 *
 */
public final class LikelihoodRankSumTest extends RankSumTest {

    @Override
    public List<String> getKeyNames() { return Collections.singletonList(GATKVCFConstants.LIKELIHOOD_RANK_SUM_KEY); }

    @Override
    public List<VCFInfoHeaderLine> getDescriptions() { return Collections.singletonList(GATKVCFHeaderLines.getInfoLine(getKeyNames().get(0))); }

    @Override
    protected OptionalDouble getElementForRead(final GATKRead read, final int refLoc, final MostLikelyAllele mostLikelyAllele) {
        Utils.nonNull(read, "read is null");
        Utils.nonNull(mostLikelyAllele, "mostLikelyAllele is null");
        if ( ! mostLikelyAllele.isInformative() ) {
            throw new IllegalStateException("Should never see a non-informative allele for read " + read + " MostLikelyAllele " + mostLikelyAllele);
        }
        return OptionalDouble.of(mostLikelyAllele.getLog10LikelihoodOfMostLikely());
    }

    @Override
    protected OptionalDouble getElementForRead(final GATKRead read, final int refLoc) {
        Utils.nonNull(read);
        throw new IllegalStateException("This method should never have been called as getElementForRead(read,refloc,mostLikelyAllele) was overriden");
    }
}