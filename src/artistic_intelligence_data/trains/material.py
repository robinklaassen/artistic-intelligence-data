from enum import StrEnum


class TrainMaterial(StrEnum):
    """Main types of 'materieel' for trains. Every type has subtypes e.g. 'VIRM IV' but they are not here."""

    VIRM = "VIRM"
    SNG = "SNG"
    SLT = "SLT"
    DDZ = "DDZ"
    FLIRT = "FLIRT"
    GTW = "GTW"
    ICM = "ICM"
    ICNG = "ICNG"
