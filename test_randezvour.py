import asyncio

from randezvour_connection import RandezvousConnection


async def main():

    rdv = RandezvousConnection(
    "45.171.101.167",
    8080
    )

    '''register_msg = {
        "type": "REGISTER",
        "namespace": "CIC",
        "name": "teste",
        "port": 5000,
        "ttl": 3600
    }

    response = await rdv.request(
        register_msg
    )'''


    discorver_msg = {
        "type": "DISCOVER",
        "namespace": "",
    }

    response = await rdv.request(
        discorver_msg
    )

    print(response)


asyncio.run(main())