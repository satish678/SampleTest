package org.tcs.GlobalWhether;

import java.rmi.RemoteException;

import NET.webserviceX.www.GlobalWeatherSoapProxy;

public class GlobalWhetherExample {

	public static void main(String[] args) throws RemoteException {
		// TODO Auto-generated method stub
		System.out.println("Client app for getting whether info from web service ");
		
		GlobalWeatherSoapProxy proxy = new GlobalWeatherSoapProxy();
		System.out.println("the cities in country : "+proxy.getCitiesByCountry("united states"));
		System.out.println("the current whether in our cityss : "+proxy.getWeather("Point Lay", "united states"));
	}

}
