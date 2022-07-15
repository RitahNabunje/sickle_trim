"""
Trim FastQ reads with Sickle
"""

import subprocess
from pathlib import Path
from typing import Annotated, List, Optional
from enum import Enum
from latch import small_task, workflow
from latch.types import LatchFile, LatchDir


class SequenceType(Enum):
    PE = "pe"
    SE = "se"


class QualityType(Enum):
    SANGER = "sanger"
    ILLUMINA = "illumina"
    SOLEXA = "solexa"


@small_task
def trim_task(read1: LatchFile, read2: Optional[LatchFile],
              sequence_type: SequenceType = SequenceType.PE,
              quality_type: QualityType = QualityType.SANGER
              ) -> LatchDir:

    out_dir = Path("/sickle_out")
    _outdir_cmd = [
        "mkdir",
        "/sickle_out"
    ]
    subprocess.run(_outdir_cmd)

    # input and output
    accepted_files = ['.fastq', '.fq', '.FASTQ', '.FQ']

    if not Path(read1).suffix in accepted_files:
        raise ValueError(f"{read1} is not a valid fastq file")
    out1 = f"{out_dir}/trimmed_{Path(read1).name}"
    out3 = f"{out_dir}/trimmed_singles{Path(read1).suffix}"

    if quality_type is QualityType.SANGER:
        quality_values = "sanger"
    elif quality_type is QualityType.ILLUMINA:
        quality_values = "illumina"
    else:
        quality_values = "solexa"

    if sequence_type is SequenceType.PE:
        out2 = f"{out_dir}/trimmed_{Path(read2).name}"
        if not Path(read2).suffix in accepted_files:
            raise ValueError(f"{read2} is not a valid fastq file")
        read_type = "pe"
        _sickle_pe_cmd = [
            "sickle", read_type,
            "-f", Path(read1),
            "-r", Path(read2),
            "-t", quality_values,
            "-o", out1,
            "-p", out2,
            "-s", out3
        ]
        subprocess.run(_sickle_pe_cmd)
    else:
        read_type = "se"
        _sickle_se_cmd = [
            "sickle", read_type,
            "-t", quality_values,
            "-f", Path(read1),
            "-o", out1
        ]
        subprocess.run(_sickle_se_cmd)

    return LatchDir(str(out_dir), f"latch:///{out_dir}")


@workflow
def sickle_trim(read1: LatchFile, read2: Optional[LatchFile],
                sequence_type: SequenceType = SequenceType.PE,
                quality_type: QualityType = QualityType.SANGER
                ) -> LatchDir:
    """Trim sequence reads using Sickle

    Sickle
    ----

    Sickle applies windowed adaptive trimming for fastq files. It uses quality and length thresholds
    to determine and trim low quality bases at both 3’ end and 5’ end of the reads.

    > Regular markdown constructs work as expected.

    # Heading

    * content1
    * content2

    __metadata__:
        display_name: SickleTrim
        author:
            name: Ritah Nabunje
            email: ritahnabunje@gmail.com
            github: https://github.com/RitahNabunje
        repository: https://github.com/RitahNabunje/sickle_trim
        license:
            id: MIT

    Args:

        read1:
          The file with the all sequence reads for single-end data or the forward reads for paired-end data

          __metadata__:
            display_name: Read1

        read2:
          The reverse reads for paired-end data

          __metadata__:
            display_name: Read2

        sequence_type:
          Type of sequence reads i.e single-end (se) or paired-end (pe)

          __metadata__:
            display_name: Sequence Type

        quality_type:
          Type of quality score values in the fastq files i.e solexa, illumina or sanger

          __metadata__:
            display_name: Quality Values

    """

    return trim_task(read1=read1, read2=read2, sequence_type=sequence_type, quality_type=quality_type)
