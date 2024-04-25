from pandas import DataFrame

from multiprocessing import Pool, cpu_count

from typing import Dict


def parse_line(args) -> Dict[str, str]:
    line, columns = args
    return {name: line[start:end].strip() for (name, start, end) in columns}


def parse_data() -> None:
    columns = [
        ("DUNS", 0, 9),
        ("DCOMP", 10, 39),
        ("DTRADE", 40, 69),
        ("DSTREET", 70, 94),
        ("DCITY", 95, 114),
        ("DSTATEAB", 115, 116),
        ("DZIP5", 117, 121),
        ("DZIP4EXT", 122, 125),
        ("DMAILADD", 126, 150),
        ("DMAILCIT", 151, 170),
        ("DMAILSTA", 171, 172),
        ("DMAILZIP", 173, 177),
        ("DMAILZP4", 178, 181),
        ("DCARRRTC", 182, 185),
        ("FILLER10", 186, 187),
        ("DNATLCOD", 188, 190),
        ("DSTATECO", 191, 192),
        ("DCOUNTYC", 193, 195),
        ("DCITYCOD", 196, 199),
        ("DSMSACOD", 200, 202),
        ("DTELEPHO", 203, 212),
        ("DCEONAME", 213, 242),
        ("DCEOTITT", 243, 272),
        ("DSALESVO", 273, 287),
        ("DSLSVOLC", 288, 288),
        ("DEMTLTOT", 289, 297),
        ("DEMTOTC", 298, 298),
        ("DEMTLHER", 299, 307),
        ("DEMPHRCDC", 308, 308),
        ("DYRSTART", 309, 312),
        ("DSTATUSI", 313, 313),
        ("DSUBSIDI", 314, 314),
        ("DMANUFIN", 315, 315),
        ("DULTDUN", 316, 324),
        ("DHDQDUN", 325, 333),
        ("DPARDUN", 334, 342),
        ("DPRHQCT", 343, 362),
        ("DPRHQST", 363, 364),
        ("FILLER1", 365, 372),
        ("FILLER2", 373, 382),
        ("DHIER", 383, 384),
        ("DDIAS", 385, 393),
        ("DPOPLCD", 394, 394),
        ("DTRANCD", 395, 395),
        ("DRPTDAT", 396, 401),
        ("FILLER3", 402, 420),
        ("DRCRDCL", 421, 421),
        ("DLINEBU", 422, 440),
        ("DPRIMSI", 441, 444),
        ("DSICEXT1", 445, 448),
        ("DSICEXT2", 449, 452),
        ("DSICEXT3", 453, 456),
        ("DSICEXT4", 457, 460),
        ("DSIC2", 461, 480),
        ("DSIC3", 481, 500),
        ("DSIC4", 501, 520),
        ("DSIC5", 521, 540),
        ("DSIC6", 541, 560),
    ]

    with open("D&B/MGNTFIS3.I9Q5VOQI.DMI.1990.TXT", "r") as file, Pool(
        cpu_count()
    ) as pool:
        lines = file.readlines()
        # results = list(executor.map(parse_line, lines, chunksize=[columns] * len(lines)))
        results = pool.map(parse_line, [(line, columns) for line in lines])

    print("Data parsed.")

    df = DataFrame(results)

    df.to_csv("D&B/MGNTFIS3.I9Q5VOQI.DMI.1990.csv", index=False)

    print("Data saved.")
