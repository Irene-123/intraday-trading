from broker import Broker, OrderManager

def main():
    broker = Broker()
    order_manager = OrderManager(broker.client)
    
    # Example: Placing an order
    scrip_code = "500325"  # Example ScripCode for Reliance Industries
    quantity = 10
    price = 2500.0
    order_type = "BUY"

    response = order_manager.place_order(scrip_code=scrip_code, quantity=quantity, price=price, order_type=order_type)
    print("Order Response:", response)
    
    if response and isinstance(response, dict) and 'order_id' in response:
        order_id = response['order_id']
    
        # Example: Modifying an order
        modified_price = 2550.0
        response = order_manager.modify_order(order_id, scrip_code, quantity, modified_price, order_type)
        print("Modify Order Response:", response)
    
        # Example: Cancelling an order
        response = order_manager.cancel_order(order_id)
        print("Cancel Order Response:", response)
    else:
        print("Failed to place order, cannot proceed with modify or cancel actions.")

if __name__ == "__main__":
    main()
